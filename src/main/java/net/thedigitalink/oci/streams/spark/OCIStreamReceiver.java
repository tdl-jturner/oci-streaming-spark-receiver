package net.thedigitalink.oci.streams.spark;

import com.google.common.util.concurrent.Uninterruptibles;
import com.oracle.bmc.auth.*;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import shaded.com.oracle.oci.javasdk.com.google.common.base.Supplier;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Custom OCI Stream Receiver for streaming data to spark
 */
public class OCIStreamReceiver extends Receiver<String> {

    public enum AuthProvider {
        INSTANCE_PRINCIPALS,
        BDC_AUTH,
        DEFAULT_CONFIG
    }

    private static Logger log = Logger.getLogger(OCIStreamReceiver.class);

    private final String groupName;
    private final String instanceName;
    private final String streamId;
    private final String streamEndpoint;
    private final int limit;
    private final AuthProvider authProvider;

    /**
     * Sample main method to test from command line
     * @param args array of arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: OCIStreamReceiver <streamId> <streamEndpoint> <groupName> <instanceName> <limit>");
            System.exit(1);
        }

        // Create the context with a 10 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("Test - OCIStreamReceiver");
        sparkConf.set("spark.master","local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000));
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(new OCIStreamReceiver(AuthProvider.INSTANCE_PRINCIPALS, args[0],args[1],args[2],args[3],Integer.parseInt(args[4])));
        lines.print();
        ssc.start();
        ssc.awaitTermination();
    }


    /**
     * Construct OCI Stream Receiver
     * @param groupName streaming group name
     * @param instanceName streaming instance name
     * @throws Exception failure to load the stream
     */
    public OCIStreamReceiver(AuthProvider authProvider, String streamId, String streamEndpoint, String groupName, String instanceName, int limit) {
        super(StorageLevel.MEMORY_AND_DISK_2());

        this.streamId = streamId;
        log.info(String.format("Setting StreamId to %s",this.streamId));
        this.streamEndpoint = streamEndpoint;
        log.info(String.format("Setting Endpoint to %s",this.streamEndpoint));
        this.groupName=groupName;
        log.info(String.format("Setting Group Name to %s",this.groupName));
        this.instanceName=instanceName;
        log.info(String.format("Setting Instance Name to %s",this.instanceName));
        this.limit=limit;
        log.info(String.format("Setting Limit to %s",this.limit));
        this.authProvider=authProvider;
        log.info(String.format("Setting Auth Provider to %s",this.authProvider));
    }

    private BasicAuthenticationDetailsProvider getAuthProvider() {
        switch(this.authProvider) {
            case BDC_AUTH:
                return getBDCAuthProvider();
            case INSTANCE_PRINCIPALS:
                return getInstancePrincipalsAuthProvider();
            case DEFAULT_CONFIG:
            default:
                return getConfigFileAuthProvider();
        }
    }

    private AuthenticationDetailsProvider  getConfigFileAuthProvider() {
        try {
            return new ConfigFileAuthenticationDetailsProvider("DEFAULT");
        }
        catch (Exception e) {
            log.error("Failed to get config file auth provider",e);
            return null;
        }
    }

    private AuthenticationDetailsProvider getBDCAuthProvider() {
        try (InputStream input = new FileInputStream("/u01/bdcsce/data/etc/bdcsce/conf/datasources.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            return getSimpleAuthProvider(
                    prop.getProperty("oci_storage_tenantid"),
                    prop.getProperty("oci_storage_userid"),
                    prop.getProperty("oci_storage_fingerprint"),
                    "/u01/bdcsce/data/etc/bdcsce/conf/oci_api_key.pem",
                    null);

        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    private AuthenticationDetailsProvider  getSimpleAuthProvider(String tenantId, String userId, String fingerprint, String pemFilePath, String passPhrase) {
        Supplier<InputStream> supplier = new SimplePrivateKeySupplier(pemFilePath);
        return SimpleAuthenticationDetailsProvider.builder()
                .tenantId(tenantId)
                .userId(userId)
                .fingerprint(fingerprint)
                .privateKeySupplier(supplier)
                .passphraseCharacters(passPhrase.toCharArray())
                .build();
    }

    private InstancePrincipalsAuthenticationDetailsProvider getInstancePrincipalsAuthProvider() {
        return InstancePrincipalsAuthenticationDetailsProvider.builder().build();
    }

    /**
     * Create a stream client
     * @return StreamClient for defined stream
     */
    private StreamClient getStreamClient() {
        StreamClient streamClient = new StreamClient(getAuthProvider());
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
