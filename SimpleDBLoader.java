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
import com.amazonaws.sdb.*;
import com.amazonaws.sdb.model.*;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

// Needed to prevent warnings on a call to the clone method on a generic ArrayList
@SuppressWarnings("unchecked")

public class SimpleDBLoader {

    /************************************************************************
    * Access Key ID and Secret Access Key ID, obtained from:
    * http://aws.amazon.com
    * You need to set this to your own keys before running this example
    ***********************************************************************/
    public String accessKeyId = "";
    public String secretAccessKey = "";

    int domainCount = 25;
    String domainPrefix = "test_domain";
    int batchCount = 20;

    ArrayList<ArrayList<ReplaceableItem>> domainItems = new ArrayList<ArrayList<ReplaceableItem>>(domainCount);
    List<Future<BatchPutAttributesResponse>> putResponses = new ArrayList<Future<BatchPutAttributesResponse>>();

    long jobStartTime = 0;
    ArrayList<Long> domainLastPutTime = new ArrayList<Long>(domainCount);

    public static void main(String[] args) {

        SimpleDBLoader loader = new SimpleDBLoader();
        if (loader.accessKeyId.equals(""))
        {
            System.out.println("*************************************************************************************************************");
            System.out.println("*You need to edit SimpleDBLoader.java to add your own AWS access keys and recompile before you can run this.*");
            System.out.println("*************************************************************************************************************");
            System.out.println("");
            loader.printHelp();
            System.exit(1);
        }
        
        String command;
        if (args.length<1)
            command = "help";
        else 
            command = args[0];

        if (command.equals("help"))
        {
            loader.printHelp();
        }
        else if (command.equals("setup"))
        {
            loader.setup();
        }
        else if (command.equals("cleanup"))
        {
            loader.cleanup();
        }
        else if (command.equals("test"))
        {
            int itemCount = 10000;
            if (args.length>1)
            {
                try {
                    itemCount = Integer.parseInt(args[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Test item count argument must be an integer");
                    loader.printHelp();
                    System.exit(1);
                }
            }

            int threadCount = 100;
            if (args.length>2)
            {
                try {
                    threadCount = Integer.parseInt(args[2]);
                } catch (NumberFormatException e) {
                    System.out.println("Test item count argument must be an integer");
                    loader.printHelp();
                    System.exit(1);
                }
            }
            
            loader.runTests(itemCount, threadCount);
        }
        else 
        {
            System.out.println("Unknown argument: "+command);
            loader.printHelp();            
        }
    }
    
    public void printHelp()
    {
        System.out.println("This project explores the fastest way of loading large amounts of data into SimpleDB.");
        System.out.println("");
        System.out.println("You'll need to edit SimpleDBLoader.java to add your own AWS keys and recompile before you can do anything.");
        System.out.println("Once you've done that, create the domains you'll need by running this script as:");
        System.out.println("java -cp<...> SimpleDBLoader setup");
        System.out.println("");
        System.out.println("After that, you can test the data loading speed by passing 'test' as the first argument, followed by an optional number of items (defaults to 10000)");
        System.out.println("and the number of threads to use (defaults to 100)");
        System.out.println("When you're finished testing, run this command to remove the test domains:");
        System.out.println("java -cp<...> SimpleDBLoader cleanup");
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
    
    public void setup()
    {
        AmazonSimpleDBConfig config = new AmazonSimpleDBConfig().withMaxConnections (100);
        ExecutorService executor = Executors.newFixedThreadPool(100);
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
        AmazonSimpleDBConfig config = new AmazonSimpleDBConfig().withMaxConnections (100);
        ExecutorService executor = Executors.newFixedThreadPool(100);
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
    
    public int getDomainForId(int id)
    {
        return (Math.abs(id)%domainCount);
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

    // Returns the current required delay between calls to the same domain, based on the idea that
    // AWS penalizes 'bursty' writers by throttling them, so we need to slowly ease up our request
    // rate from 1 per second at the start, to around 5 over the course of two minutes
    public long getDesiredDelay(int domainIndex)
    {
        long currentTime = (System.currentTimeMillis()-jobStartTime);
        
        // Ramp up over 120 seconds
        float maxTime = (120*1000);
        
        // Start with one request per second
        float startValue = 1.0f;
        
        // And finish with five requests per second
        float endValue = 5.0f;
        
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

    public void runTests(int itemCount, int threadCount) 
    {        
        AmazonSimpleDBConfig config = new AmazonSimpleDBConfig().withMaxConnections (threadCount);
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

        // Create the requested number of items and add them to SimpleDB in batches
        for (int index=0; index<itemCount; index+=1)
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
