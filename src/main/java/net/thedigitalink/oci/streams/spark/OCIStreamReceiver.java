package net.thedigitalink.oci.streams.spark;

import com.google.common.util.concurrent.Uninterruptibles;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Custom OCI Stream Receiver for streaming data to spark
 */
public class OCIStreamReceiver extends Receiver<String> {

    private static Logger log = Logger.getLogger(OCIStreamReceiver.class);

    private final String groupName;
    private final String instanceName;
    private final String streamId;
    private final String streamEndpoint;
    private final String compartmentId;
    private final int limit;

    /**
     * Sample main method to test from command line
     * @param args array of arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: OCIStreamReceiver <compartmentId> <groupName> <instanceName> <streamName> <limit>");
            System.exit(1);
        }


        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("Test - OCIStreamReceiver");
        sparkConf.set("spark.master","local");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(new OCIStreamReceiver(args[0],args[1],args[2],args[3],Integer.parseInt(args[4])));
        lines.print();
        ssc.start();
        ssc.awaitTermination();
    }


    /**
     * Construct OCI Stream Receiver
     * @param compartmentId OCID of compartment
     * @param groupName streaming group name
     * @param instanceName streaming instance name
     * @param streamName stream name
     * @throws Exception failure to load the stream
     */
    public OCIStreamReceiver(String compartmentId, String groupName, String instanceName, String streamName, int limit) throws Exception{
        super(StorageLevel.MEMORY_AND_DISK_2());

        this.compartmentId = compartmentId;
        log.info(String.format("Setting compartmentId to %s",this.compartmentId));

        Stream stream = getStream(compartmentId, streamName);

        this.streamId = stream.getId();
        log.info(String.format("Setting StreamId to %s",this.streamId));
        this.streamEndpoint = stream.getMessagesEndpoint();
        log.info(String.format("Setting Endpoint to %s",this.streamEndpoint));
        this.groupName=groupName;
        log.info(String.format("Setting Group Name to %s",this.groupName));
        this.instanceName=instanceName;
        log.info(String.format("Setting Instance Name to %s",this.instanceName));
        this.limit=limit;
        log.info(String.format("Setting Limit to %s",this.limit));
    }

    /**
     * Lookup stream
     * @param compartmentId Compartment ID
     * @param streamName Name of Stream
     * @return Stream object
     */
    private Stream getStream(String compartmentId, String streamName)  {
        try {
            InstancePrincipalsAuthenticationDetailsProvider provider = InstancePrincipalsAuthenticationDetailsProvider.builder().build();

            StreamAdminClient adminClient = new StreamAdminClient(provider);
            ListStreamsRequest listRequest =
                    ListStreamsRequest.builder()
                            .compartmentId(compartmentId)
                            .lifecycleState(Stream.LifecycleState.Active)
                            .name(streamName)
                            .build();

            ListStreamsResponse listResponse = adminClient.listStreams(listRequest);

            if (listResponse.getItems().isEmpty()) {
                throw new AssertionError(String.format("Stream {} does not exist", streamName));
            }
            String streamId = listResponse.getItems().get(0).getId();


            return adminClient.getStream(GetStreamRequest.builder().streamId(streamId).build()).getStream();
        }
        catch (Exception e) {
            log.error("Failed to get Stream",e);
            return null;
        }
    }

    /**
     * Create a stream client
     * @return StreamClient for defined stream
     */
    private StreamClient getStreamClient() {
        InstancePrincipalsAuthenticationDetailsProvider provider = InstancePrincipalsAuthenticationDetailsProvider.builder().build();
        StreamClient streamClient = new StreamClient(provider);
        streamClient.setEndpoint(this.streamEndpoint);
        return streamClient;
    }

    /**
     * Gets a new group cursor using the provided configuration
     * @param streamClient client to use to pull cursor
     * @return cursor string
     */
    private String getCursor(StreamClient streamClient) {
        CreateGroupCursorDetails cursorDetails =
                CreateGroupCursorDetails.builder()
                        .groupName(groupName)
                        .instanceName(instanceName)
                        .type(CreateGroupCursorDetails.Type.TrimHorizon)
                        .commitOnGet(true)
                        .build();

        CreateGroupCursorRequest createCursorRequest =
                CreateGroupCursorRequest.builder()
                        .streamId(streamId)
                        .createGroupCursorDetails(cursorDetails)
                        .build();

        CreateGroupCursorResponse groupCursorResponse = streamClient.createGroupCursor(createCursorRequest);

        return groupCursorResponse.getCursor().getValue();
    }


    /**
     * Starts Receive thread
     */
    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread(this::receive).start();
    }

    /**
     * Stop procedure - Empty
     */
    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false
    }

    /**
     * Process stream and receive message
     */
    private void receive() {

        StreamClient streamClient = null;

        try {

            streamClient=getStreamClient();

            String cursor=getCursor(streamClient);

            while (!isStopped()) {
                GetMessagesRequest getRequest =
                        GetMessagesRequest.builder()
                                .streamId(streamId)
                                .cursor(cursor)
                                .limit(this.limit)
                                .build();
                GetMessagesResponse getResponse = streamClient.getMessages(getRequest);
                for (Message message : getResponse.getItems()) {
                    String key = null;
                    if(message.getKey()!=null) {
                        key = new String(message.getKey(), UTF_8);
                    }
                    String value = null;
                    if(message.getValue()!=null) {
                        value = new String(message.getValue(), UTF_8);
                    }
                    String messageString = String.format("%s: %s", key,value);
                    log.info(String.format("Received Message: %s", messageString));
                    store(messageString);

                }
                log.debug("Sleeping");
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                cursor = getResponse.getOpcNextCursor();
            }
        }
        catch (Exception e) {
            restart("Error receiving data", e);
        }
        finally {
            if(streamClient!=null) {
                streamClient.close();
            }
        }

    }
}
