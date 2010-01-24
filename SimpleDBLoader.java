/******************************************************************************* 
 *
 * An example and speed-test of bulk data loading into SimpleDB
 * Based on the BatchPutAttributesAsync sample code included in the SimpleDB Java library
 * By Pete Warden <pete@petewarden.com> - freely reusable with no conditions
 * See http://petewarden.typepad.com for more information
 * 
 */

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import com.amazonaws.sdb.*;
import com.amazonaws.sdb.model.*;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.*;

class ItemToAdd {
    public ReplaceableItem item;
    public int domainIndex;
    
    public ItemToAdd(ReplaceableItem inItem, int inDomainIndex) {
        item = inItem;
        domainIndex = inDomainIndex;
    }
}

// These are the two functions that any data source needs to implement to plug into the SimpleDB loader
interface ItemGenerator {
    boolean hasMoreItems();
    ItemToAdd getNextItem();
}

// This data source creates a set number of quasi-random objects, and is used to test the loader
class TestItemGenerator implements ItemGenerator {

    int index;
    int itemCount;
    int domainCount;
    
    public TestItemGenerator(int inItemCount, int inDomainCount) {
        index = 0;
        itemCount = inItemCount;
        domainCount = inDomainCount;
    }
    
    public boolean hasMoreItems()
    {
        return (index<itemCount);
    }
    
    // See http://www.concentric.net/~Ttwang/tech/inthash.htm for the source of this hash function
    public int hashFunction(int input)
    {
        int key = ~input + (input << 15); // key = (key << 15) - key - 1;
        key = key ^ (key >>> 12);
        key = key + (key << 2);
        key = key ^ (key >>> 4);
        key = key * 2057; // key = (key + (key << 3)) + (key << 11);
        key = key ^ (key >>> 16);
        return key;
    }

    public int getDomainForId(int id)
    {
        return (Math.abs(id)%domainCount);
    }

    public ItemToAdd getNextItem()
    {
        // Try to generate a fairly random id for each index, so that we don't end up
        // filling each bucket at the same time and send them off at staggered intervals.
        // This is closer to the real-world distribution of ids, and makes a better test.
        int id = hashFunction(index);
        int domainIndex = getDomainForId(id);

        ArrayList<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();

        attributes.add(new ReplaceableAttribute("first", Integer.toString(id), false));
        attributes.add(new ReplaceableAttribute("second", Integer.toString(id*2), false));
        attributes.add(new ReplaceableAttribute("third", "{a:'foo', b:'bar'}", false));
        attributes.add(new ReplaceableAttribute("fourth", "[10,9,8,7,6,5,4,3,2,1]", false));
        
        ReplaceableItem item = new ReplaceableItem(Integer.toString(id), attributes);
        
        ItemToAdd result = new ItemToAdd(item, domainIndex);
        
        index += 1;
        
        return result;
    }
}

// Loads data from a file, consisting of lines starting with a string for the id, 
// followed by a tab, then a JSON string representing the item's attributes
class JSONFileItemGenerator implements ItemGenerator {

    BufferedReader reader;
    int domainCount;
    
    public JSONFileItemGenerator(String filename, int inDomainCount) 
    {
        try 
        {
            FileInputStream inputStream = new FileInputStream(filename);
            reader = new BufferedReader(new InputStreamReader(inputStream));
        }
        catch (FileNotFoundException exception)
        {
            System.err.println("File could not be read: "+filename);
            System.exit(1);
        }

        domainCount = inDomainCount;
    }
    
    public boolean hasMoreItems()
    {
        try
        {
            return reader.ready();
        }
        catch (IOException exception)
        {
            return false;
        }
    }
    
    public int hashFunction(String key)
    {
        CRC32 crc = new CRC32();
        crc.update(key.getBytes());
        return (int)(crc.getValue());
    }
    
    public int getDomainForKey(String key)
    {
        int hash = hashFunction(key);
        return (Math.abs(hash)%domainCount);
    }
    
    public ItemToAdd getNextItem()
    {
        String line = "";
        try 
        {
            line = reader.readLine();
        }
        catch (IOException exception)
        {
            System.err.println("Error reading from file");
            System.exit(1);
        }

        String [] parts = line.split("\t", 2);
            
        String key;
        String dataString;
        if (parts.length<2)
        {
            System.err.println("Bad line found:"+line);
            key = line;
            dataString = "{}";
        }
        else 
        {
            key = parts[0];
            dataString = parts[1];
        }

        JSONObject data = (JSONObject)(JSONValue.parse(dataString));

        int domainIndex = getDomainForKey(key);

        ArrayList<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();

        Iterator it = data.entrySet().iterator();
        while (it.hasNext()) 
        {
            Map.Entry pairs = (Map.Entry)it.next();

            String baseName = pairs.getKey().toString();
            String value = JSONValue.toJSONString(pairs.getValue());

            final int maxLength = 1020;

            if (value.length()<maxLength)
            {
                attributes.add(new ReplaceableAttribute(baseName, value, false));
            }
            else 
            {
                int index = 0;
                while (value.length()>0)
                {
                    String subValue = value.substring(0, Math.min(maxLength, value.length()));
                    String name;
                    if (index==0)
                        name = baseName;
                    else
                        name = baseName+"*"+Integer.toString(index);
                    
                    attributes.add(new ReplaceableAttribute(name, subValue, false));
            
                    value = value.substring(Math.min(maxLength, value.length()));
                    index += 1;
                }

                
            }

        }            
        ReplaceableItem item = new ReplaceableItem(key, attributes);
        
        ItemToAdd result = new ItemToAdd(item, domainIndex);
        
        return result;    
    }
}

// Needed to prevent warnings on a call to the clone method on a generic ArrayList
@SuppressWarnings("unchecked")
public class SimpleDBLoader {

    /************************************************************************
    * Access Key ID and Secret Access Key ID, obtained from:
    * http://aws.amazon.com
    * You need to set this to your own keys before running this example,
    * or supply your own credentials on the command line
    ***********************************************************************/
    public String accessKeyId = "";
    public String secretAccessKey = "";

    int domainCount = 25;
    String domainPrefix = "test_domain";
    int batchCount = 20;
    int threadCount = 100;
    float minRPS = 1.0f;
    float maxRPS = 3.0f;
    long rampTime = (120*1000);

    int itemCount = 10000;
    String filename = "";

    ArrayList<ArrayList<ReplaceableItem>> domainItems = new ArrayList<ArrayList<ReplaceableItem>>(domainCount);
    List<Future<BatchPutAttributesResponse>> putResponses = new ArrayList<Future<BatchPutAttributesResponse>>();

    long jobStartTime = 0;
    ArrayList<Long> domainLastPutTime = new ArrayList<Long>(domainCount);

    public static void main(String[] args) {

        SimpleDBLoader loader = new SimpleDBLoader();
        
        String command = loader.parseArguments(args);

        if (command.equals("help"))
        {
            loader.printHelp();
            System.exit(1);
        }
        
        if (loader.accessKeyId.equals(""))
        {
            System.out.println("*****************************************************************************************");
            System.out.println("*You need to supply your access keys vias the command line, or edit SimpleDBLoader.java*");
            System.out.println("*to add your own AWS access keys and recompile before you can run this.                *");
            System.out.println("****************************************************************************************");
            System.out.println("");
            loader.printHelp();
            System.exit(1);
        }
        
        if (command.equals("setup"))
        {
            loader.setup();
        }
        else if (command.equals("cleanup"))
        {
            loader.cleanup();
        }
        else if (command.equals("test"))
        {
            loader.runTests();
        }
        else if (command.equals("loadjson"))
        {
            if (loader.filename.equals(""))
            {
                System.out.println("You must supply a filename to load the data from.");
                loader.printHelp();
                System.exit(1);
            }
            loader.loadJSONFile();
        }
        else 
        {
            System.out.println("Unknown argument: "+command);
            loader.printHelp();            
        }
    }
    
    public String parseArguments(String[] args)
    {
        String command;
        if (args.length<1)
            command = "help";
        else 
            command = args[0].toLowerCase();
        
        if (command.equals("help"))
            return command;
            
        for (int index=1; index<args.length;)
        {
            String currentToken = args[index];
            index += 1;
            
            if (!currentToken.startsWith("-"))
            {
                System.out.println("Unexpected argument: "+currentToken);
                printHelp();
                System.exit(1);
            }
        
            String fullArgName = currentToken.substring(1);
            String [] argParts = fullArgName.split("=", 2);

            String argName;
            String argValue;
            if (argParts.length<2)
            {
                argName = fullArgName;
                
                if (index>=args.length)
                {
                    System.out.println("Missing value for argument: "+currentToken);
                    printHelp();
                    System.exit(1);
                }
                
                argValue = args[index];
                index += 1;
            }
            else 
            {
                argName = argParts[0];
                argValue = argParts[1];
            }

            if (argName.equals("domaincount")||argName.equals("d"))
            {
                domainCount = Integer.parseInt(argValue);
            }
            else if (argName.equals("domainprefix")||argName.equals("p"))
            {
                domainPrefix = argValue;
            }
            else if (argName.equals("threadcount")||argName.equals("t"))
            {
                threadCount = Integer.parseInt(argValue);
            }            
            else if (argName.equals("itemcount")||argName.equals("i"))
            {
                itemCount = Integer.parseInt(argValue);
            }
            else if (argName.equals("batchcount")||argName.equals("b"))
            {
                batchCount = Integer.parseInt(argValue);
            }
            else if (argName.equals("filename")||argName.equals("f"))
            {
                filename = argValue;
            }
            else if (argName.equals("maxrps")||argName.equals("m"))
            {
                maxRPS = Float.parseFloat(argValue);
            }
            else if (argName.equals("minrps")||argName.equals("n"))
            {
                maxRPS = Float.parseFloat(argValue);
            }
            else if (argName.equals("ramptime")||argName.equals("r"))
            {
                rampTime = Integer.parseInt(argValue)*1000;
            }
            else if (argName.equals("accesskey")||argName.equals("a"))
            {
                accessKeyId = argValue;
            }            
            else if (argName.equals("secretkey")||argName.equals("s"))
            {
                secretAccessKey = argValue;
            }            
            else
            {
                System.out.println("Unexpected argument: "+currentToken);
                printHelp();
                System.exit(1);
            }
        
        }
        
        return command;
    }
    
    public void printHelp()
    {
        System.out.println("");
        System.out.println("This project explores the fastest way of loading large amounts of data into SimpleDB.");
        System.out.println("");
        System.out.println("First, create the domains you'll need by running this script as:");
        System.out.println("./sdbloader setup");
        System.out.println("");
        System.out.println("After that, you can test the data loading speed by passing 'test' as the first argument");
        System.out.println("When you're finished testing, run this command to remove the test domains:");
        System.out.println("./sdbloader cleanup");
        System.out.println("");
        System.out.println("This class also functions as a fast way to upload data from files. You can pass in 'loadjson' as the first argument,");
        System.out.println("followed by -filename/-f <filename>, and the file's data will be uploaded.");
        System.out.println("The JSON format for uploading is to have each item's data on a separate line in the file. The first string is the unique ID to use");
        System.out.println("as the item's key, followed by a tab, then a JSON string containing an associative array describing that item's attributes and values.");
        System.out.println("");
        System.out.println("You can supply the following arguments to configure the loading:");
        System.out.println("-domaincount/-d : How many domains to shard the items across (defaults to 25)");
        System.out.println("-domainprefix/-p : The string to start all the domain names with, followed by a number for each (defaults to 'test_domain')");
        System.out.println("-threadcount/-t : The number of threads to run in parallel (defaults to 100)");
        System.out.println("-itemcount/-i : For the 'test' benchmarking command, sets the number of items to generate and load (defaults to 10000)");
        System.out.println("-batchcount/-b : How many items to batch together into a single HTTP request to SimpleDB (defaults to 20)");
        System.out.println("-filename/-f : For data loading commands, the filename to read the data from");
        System.out.println("-minrps/-n : To avoid throttling, we need to slowly ramp up our request speed. This is the number of requests per-domain, per-second to start with (defaults to 1.0)");
        System.out.println("-maxrps/-m : The maximum number of requests per-domain, per-second to ramp up to with (defaults to 3.0)");
        System.out.println("-ramptime/-r : How long to ramp up to the maximum rate, in seconds (defaults to 120)");
        System.out.println("-accesskey/-a : Your AWS account access key, required");
        System.out.println("-secretkey/-s : Your AWS secret key, required");
        System.out.println("");
        
        System.out.println("By Pete Warden <pete@petewarden.com> - see http://petewarden.typepad.com/ for more details");
    }
    
    public String getDomainNameForIndex(int index)
    {
        if (index<10)
            return domainPrefix+'0'+index;
        else
            return domainPrefix+index;
    }
    
    public AmazonSimpleDBConfig getConfig()
    {
        AmazonSimpleDBConfig config = new AmazonSimpleDBConfig().withMaxConnections(threadCount);
        config.setMaxErrorRetry(5);
        
        return config;
    }
    
    public void setup()
    {
        AmazonSimpleDBConfig config = getConfig();
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AmazonSimpleDBAsync service = new AmazonSimpleDBAsyncClient(accessKeyId, secretAccessKey, config, executor);

        List<CreateDomainRequest> requests = new ArrayList<CreateDomainRequest>();
        for (int index=0; index<domainCount; index+=1)
        {
            CreateDomainRequest request = new CreateDomainRequest();
            request.setDomainName(getDomainNameForIndex(index));
            requests.add(request);
        }
    
        invokeCreateDomain(service, requests);
        
        executor.shutdown();
    }
    
    public void cleanup()
    {
        AmazonSimpleDBConfig config = getConfig();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AmazonSimpleDBAsync service = new AmazonSimpleDBAsyncClient(accessKeyId, secretAccessKey, config, executor);

        List<DeleteDomainRequest> requests = new ArrayList<DeleteDomainRequest>();
        for (int index=0; index<domainCount; index+=1)
        {
            DeleteDomainRequest request = new DeleteDomainRequest();
            request.setDomainName(getDomainNameForIndex(index));
            requests.add(request);
        }
    
        invokeDeleteDomain(service, requests);
        
        executor.shutdown();    
    }
    
    // Returns the current required delay between calls to the same domain, based on the idea that
    // AWS penalizes 'bursty' writers by throttling them, so we need to slowly ease up our request
    // rate from 1 per second at the start, to around 4 over the course of two minutes
    public long getDesiredDelay(int domainIndex)
    {
        long currentTime = (System.currentTimeMillis()-jobStartTime);
        
        // Ramp up over 120 seconds
        float maxTime = rampTime;
        
        // Start with one request per second
        float startValue = minRPS;
        
        // And finish with four requests per second
        float endValue = maxRPS;
        
        float currentRPS;
        if (currentTime>maxTime)
            currentRPS = endValue;
        else
            currentRPS = startValue + ((currentTime/maxTime)*(endValue-startValue));
            
        long desiredDelay = (long)(1000/currentRPS);
        
        return desiredDelay;
    }
    
    // This function implements the throttling behavior we need to avoid being marked as 'bursty'
    public void throttlePut(int domainIndex)
    {        
        long currentTime = (System.currentTimeMillis()-jobStartTime);
           
        long timeSinceLastPut = domainLastPutTime.get(domainIndex);
        
        long currentDelay = (currentTime-timeSinceLastPut);
        
        long desiredDelay = getDesiredDelay(domainIndex);
        
        if (currentDelay<desiredDelay)
        {
            try {
                Thread.sleep(desiredDelay-currentDelay);
            } catch (Exception e) {
                // do nothing
            }
            currentTime = (System.currentTimeMillis()-jobStartTime);
        }
        
        domainLastPutTime.set(domainIndex, currentTime);
    }

    public void startBatch(int domainIndex, AmazonSimpleDBAsync service)
    {
        throttlePut(domainIndex);
    
        String domainName = getDomainNameForIndex(domainIndex);
        ArrayList<ReplaceableItem> batchItems = 
            (ArrayList<ReplaceableItem>)(domainItems.get(domainIndex).clone());
        
        BatchPutAttributesRequest request =  new BatchPutAttributesRequest(domainName, batchItems);

        List<BatchPutAttributesRequest> requests = new ArrayList<BatchPutAttributesRequest>();
        requests.add(request);

        startBatchPutAttributes(service, requests);

        domainItems.set(domainIndex, new ArrayList<ReplaceableItem>());    
    }

    public void runTests() 
    {
        ItemGenerator generator = new TestItemGenerator(itemCount, domainCount);
        
        runLoading(generator);
    }
    
    public void loadJSONFile()
    {
        ItemGenerator generator = new JSONFileItemGenerator(filename, domainCount);
        
        runLoading(generator);
    }
    
    public void runLoading(ItemGenerator generator)
    {
        AmazonSimpleDBConfig config = getConfig();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AmazonSimpleDBAsync service = new AmazonSimpleDBAsyncClient(accessKeyId, secretAccessKey, config, executor);
    
        System.out.println("Loading items...");
        long startTime = System.currentTimeMillis();        

        jobStartTime = startTime;
        domainLastPutTime.clear();

        // Create empty batch buffers for each domain
        for (int domainIndex=0; domainIndex<domainCount; domainIndex+=1)
        {
            domainItems.add(new ArrayList<ReplaceableItem>());
            domainLastPutTime.add(new Long(0));
        }

        int itemCount = 0;
        // Create the requested number of items and add them to SimpleDB in batches
        while (generator.hasMoreItems())
        {
            ItemToAdd itemToAdd = generator.getNextItem();
            ReplaceableItem item = itemToAdd.item;
            int domainIndex = itemToAdd.domainIndex;
            
            itemCount += 1;
            
            domainItems.get(domainIndex).add(item);
            if (domainItems.get(domainIndex).size()>batchCount)
            {
                startBatch(domainIndex, service);
            }
        }

        // Write out any half-filled item buffers
        for (int domainIndex=0; domainIndex<domainCount; domainIndex+=1)
        {
            if (domainItems.get(domainIndex).size()>0)
            {
                startBatch(domainIndex, service);
            }
        }
        
        waitForFinishBatchPutAttributes();

        executor.shutdown();

        long endTime = System.currentTimeMillis();        
        long elapsed = (endTime-startTime);
        float elapsedSeconds = (elapsed/1000.0f);
        float itemsPerSecond = (itemCount/elapsedSeconds);
        System.out.println("Took "+elapsedSeconds+" seconds for "+itemCount+" items ("+itemsPerSecond+" items per second)");
    }

    // This function adds the request into the queue
    public void startBatchPutAttributes(AmazonSimpleDBAsync service, List<BatchPutAttributesRequest> requests) {
        for (BatchPutAttributesRequest request : requests) {
            
            putResponses.add(service.batchPutAttributesAsync(request));
        }
    }
    
    // After all the requests have been added to the queue, wait for them to complete
    public void waitForFinishBatchPutAttributes() {
        for (Future<BatchPutAttributesResponse> future : putResponses) {
            while (!future.isDone()) {
                Thread.yield();
            }
            try {
                BatchPutAttributesResponse response = future.get();
                // Original request corresponding to this response, if needed:
                //BatchPutAttributesRequest originalRequest = putRequests.get(responses.indexOf(future));
                // System.out.println("Response request id: " + response.getResponseMetadata().getRequestId());
            } catch (Exception e) {
                if (e.getCause() instanceof AmazonSimpleDBException) {
                    AmazonSimpleDBException exception = AmazonSimpleDBException.class.cast(e.getCause());
                    System.out.println("Caught Exception: " + exception.getMessage());
                    System.out.println("Response Status Code: " + exception.getStatusCode());
                    System.out.println("Error Code: " + exception.getErrorCode());
                    System.out.println("Error Type: " + exception.getErrorType());
                    System.out.println("Request ID: " + exception.getRequestId());
                    System.out.print("XML: " + exception.getXML());
                } else {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Create Domain request sample
     * The CreateDomain operation creates a new domain. The domain name must be unique
     * among the domains associated with the Access Key ID provided in the request. The CreateDomain
     * operation may take 10 or more seconds to complete.
     *   
     * @param service instance of AmazonSimpleDB service
     * @param requests list of requests to process
     */
    public static void invokeCreateDomain(AmazonSimpleDBAsync service, List<CreateDomainRequest> requests) {
        List<Future<CreateDomainResponse>> responses = new ArrayList<Future<CreateDomainResponse>>();
        for (CreateDomainRequest request : requests) {
            responses.add(service.createDomainAsync(request));
        }
        for (Future<CreateDomainResponse> future : responses) {
            while (!future.isDone()) {
                Thread.yield();
            }
            try {
                CreateDomainResponse response = future.get();
                // Original request corresponding to this response, if needed:
                CreateDomainRequest originalRequest = requests.get(responses.indexOf(future));
                // System.out.println("Response request id: " + response.getResponseMetadata().getRequestId());
            } catch (Exception e) {
                if (e.getCause() instanceof AmazonSimpleDBException) {
                    AmazonSimpleDBException exception = AmazonSimpleDBException.class.cast(e.getCause());
                    System.out.println("Caught Exception: " + exception.getMessage());
                    System.out.println("Response Status Code: " + exception.getStatusCode());
                    System.out.println("Error Code: " + exception.getErrorCode());
                    System.out.println("Error Type: " + exception.getErrorType());
                    System.out.println("Request ID: " + exception.getRequestId());
                    System.out.print("XML: " + exception.getXML());
                } else {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Delete Domain request sample
     * The DeleteDomain operation deletes a domain. Any items (and their attributes) in the domain
     * are deleted as well. The DeleteDomain operation may take 10 or more seconds to complete.
     *   
     * @param service instance of AmazonSimpleDB service
     * @param requests list of requests to process
     */
    public static void invokeDeleteDomain(AmazonSimpleDBAsync service, List<DeleteDomainRequest> requests) {
        List<Future<DeleteDomainResponse>> responses = new ArrayList<Future<DeleteDomainResponse>>();
        for (DeleteDomainRequest request : requests) {
            responses.add(service.deleteDomainAsync(request));
        }
        for (Future<DeleteDomainResponse> future : responses) {
            while (!future.isDone()) {
                Thread.yield();
            }
            try {
                DeleteDomainResponse response = future.get();
                // Original request corresponding to this response, if needed:
                DeleteDomainRequest originalRequest = requests.get(responses.indexOf(future));
                // System.out.println("Response request id: " + response.getResponseMetadata().getRequestId());
            } catch (Exception e) {
                if (e.getCause() instanceof AmazonSimpleDBException) {
                    AmazonSimpleDBException exception = AmazonSimpleDBException.class.cast(e.getCause());
                    System.out.println("Caught Exception: " + exception.getMessage());
                    System.out.println("Response Status Code: " + exception.getStatusCode());
                    System.out.println("Error Code: " + exception.getErrorCode());
                    System.out.println("Error Type: " + exception.getErrorType());
                    System.out.println("Request ID: " + exception.getRequestId());
                    System.out.print("XML: " + exception.getXML());
                } else {
                    e.printStackTrace();
                }
            }
        }
    }                    
}
