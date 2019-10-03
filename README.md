# OCI Spark Streaming Receiver

This artefact implements a CustomReceiver to stream OCI Streams in Spark.

# Shaded API
Due to conflicts with Spark dependencies. This project uses the [Shaded Java SDK](https://github.com/oracle/oci-java-sdk/tree/master/bmc-shaded/bmc-shaded-full
). Installation is wrapped with the install_shaded shell script.

# Sample Job
The jar contains a sample streaming job which could be submitted with something like:
spark-submit --class net.thedigitalink.oci.streams.spark.OCIStreamReceiver 
/path/to/jar/oci-streaming-spark-receiver-0.1-SNAPSHOT-jar-with-dependencies.jar  
"ocid1.stream.xxxxx" "https://api.cell-1.us-phoenix-1.streaming.oci.oraclecloud.com" "MyGroupName" "MyInstanceName" 10

# Usage
A sample scala job could be done with something like this

```scala
Logger.getRootLogger.setLevel(Level.INFO)
val conf = new SparkConf().setAppName("Streaming Example").setMaster("local[2]");
// Create a Scala Spark Context.
val sc = new SparkContext(conf)
val ssc = new StreamingContext(sc, Seconds(10))
val customReceiverStream = ssc.receiverStream(
  new OCIStreamReceiver(
    OCIStreamReceiver.AuthProvider.INSTANCE_PRINCIPALS,
    "<STREAM_ID>",
    "<STREAM_ENDPOINT>",
    "<GROUP_NAME>",
    "<INSTANCE_NAME>",
    100))
customReceiverStream.print()
ssc.start()
ssc.awaitTermination()
```


# Note: Installing In OCI Big Data Compute
There appears to be security conflicts and the APK will only run if stored in the library folder. This is likely fixable by modifying the java security policy. However, the compiled jar can simply be placed in the following locations:
- /u01/bdcsce/opt/oracle/bdcsce/current/lib/
- /u01/bdcsce/opt/oracle/bdcsce/snap/jars/
- /u01/bdcsce/usr/hdp/2.4.2.0-258/zeppelin-spark21/local-repo/2C4U48MY3_spark2/
