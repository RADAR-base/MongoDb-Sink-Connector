# Kafka MongoDb Sink Connector

[![Build Status](https://travis-ci.org/RADAR-base/MongoDb-Sink-Connector.svg?branch=master)](https://travis-ci.org/RADAR-base/MongoDb-Sink-Connector)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/64eb2330ca7146fcb0b823816f44fcb8)](https://www.codacy.com/app/RADAR-base/RADAR-MongoDbConnector?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-base/RADAR-MongoDbConnector&amp;utm_campaign=Badge_Grade)

The MongoDB-Sink-Connector is a Kafka-Connector for scalable and reliable data streaming from a Kafka topic or number of Kafka topics to a MongoDB collection or number of MongoDB collections.
It consumes Avro data from Kafka topics, converts them into Documents and inserts them into MongoDB collections.
 
Currently, it supports records that have an Avro schema.

## Installation

This connector can be used inside a Docker stack or installed as a general Kafka Connect plugin.

### Docker installation

Use the [radarbase/kafka-connect-mongodb-sink](https://hub.docker.com/r/radarbase/kafka-connect-mongodb-sink) Docker image to connect it inside a Docker infrastructure. For example, RADAR-Docker uses a [Docker Compose file](https://github.com/RADAR-base/RADAR-Docker/blob/backend-integration/dcompose-stack/radar-cp-hadoop-stack/docker-compose.yml). The Kafka Connect Docker image requires environment to be set up. In RADAR-Docker, the following environment variables are set:

```yaml
environment:
  CONNECT_BOOTSTRAP_SERVERS: PLAINTEXT://kafka-1:9092,PLAINTEXT://kafka-2:9092,PLAINTEXT://kafka-3:9092
  CONNECT_REST_PORT: 8083
  CONNECT_GROUP_ID: "mongodb-sink"
  CONNECT_CONFIG_STORAGE_TOPIC: "mongodb-sink.config"
  CONNECT_OFFSET_STORAGE_TOPIC: "mongodb-sink.offsets"
  CONNECT_STATUS_STORAGE_TOPIC: "mongodb-sink.status"
  CONNECT_KEY_CONVERTER: "io.confluent.connect.avro.AvroConverter"
  CONNECT_VALUE_CONVERTER: "io.confluent.connect.avro.AvroConverter"
  CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry-1:8081"
  CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry-1:8081"
  CONNECT_INTERNAL_KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
  CONNECT_INTERNAL_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
  CONNECT_OFFSET_STORAGE_FILE_FILENAME: "/tmp/mongdb-sink.offset"
  CONNECT_REST_ADVERTISED_HOST_NAME: "radar-mongodb-connector"
  CONNECT_ZOOKEEPER_CONNECT: zookeeper-1:2181
  CONNECT_LOG4J_LOGGERS: "org.reflections=ERROR"
  KAFKA_BROKERS: 3
```

Before starting the streams, the Docker image waits for `KAFKA_BROKERS` number of brokers to be available as well as the schema registry.

### System installation

This connector requires the following setup:
- Confluent platform installed. This connector uses the Confluent 4.0.0 platform.
- MongoDB instance installed and running with access credentials.

To install the connector, follow the next steps:

- Build MongoDB-Sink-Connector from source  or you can download the latest release from [here](https://github.com/RADAR-base/MongoDb-Sink-Connector/releases).

    ```shell
    git clone https://github.com/RADAR-base/MongoDb-Sink-Connector.git
    
    cd MongoDb-Sink-Connector
    
    ./gradlew clean build
    ```

- Follow [Confluent platform Quick start ](http://docs.confluent.io/4.0.0/quickstart.html) to start zookeeper, kafka-server, schema-registry and kafka-rest to easily stream data to a Kafka topic

    ```shell
    # Start zookeeper
    zookeeper-server-start /etc/kafka/zookeeper.properties
    
    # Start kafka-broker
    kafka-server-start /etc/kafka/server.properties
    
    # Start schema-registry
    schema-registry-start /etc/schema-registry/schema-registry.properties
    
    # Start kafka-rest
    kafka-rest-start /etc/kafka-rest/kafka-rest.properties
    ```

- Install and start MongoDB

- Add `build/libs/*` and `build/third-party/*` to a new directory in the Connect plugin path

    ```shell
    mkdir /usr/local/share/kafka-connect/plugins/kafka-connect-mongodb-sink
    cp build/libs/* build/third-party/* /usr/local/share/kafka-connect/plugins/kafka-connect-mongodb-sink
    ```
    
## Usage

Modify [sink.properties](https://github.com/RADAR-base/MongoDb-Sink-Connector/blob/master/sink.properties) file according your environment. The following properties are supported:

<table class="data-table"><tbody>
<tr>
<th>Name</th>
<th>Description</th>
<th>Type</th>
<th>Default</th>
<th>Valid Values</th>
<th>Importance</th>
</tr>
<tr>
<td>topics</td></td><td>List of topics to be streamed.</td></td><td>list</td></td><td></td></td><td></td></td><td>high</td></td></tr>
<tr>
<td>record.converter.class</td></td><td>RecordConverterFactory that returns classes to convert Kafka SinkRecords to BSON documents.</td></td><td>class</td></td><td>org.radarcns.connect.mongodb.serialization.RecordConverterFactory</td></td><td>Class extending org.radarcns.connect.mongodb.serialization.RecordConverterFactory</td></td><td>medium</td></td></tr>
<tr>
<td>batch.flush.ms</td></td><td>Flush a batch after this amount of milliseconds.</td></td><td>int</td></td><td>15000</td></td><td>[0,...]</td></td><td>low</td></td></tr>
<tr>
<td>batch.size</td></td><td>Batch size to initiate a MongoDB write operation. If the buffer does not reach this capacity within batch.flush.ms, it will be written anyway.</td></td><td>int</td></td><td>2500</td></td><td>[1,...]</td></td><td>low</td></td></tr>
<tr>
<td>buffer.capacity</td></td><td>Maximum number of items in a MongoDB writer buffer. Once the buffer becomes full, the task fails.</td></td><td>int</td></td><td>20000</td></td><td>[1,...]</td></td><td>low</td></td></tr>
<tr>
<td>mongo.uri</td></td><td>URI encoding MongoDB host, port, user (if any) and database.</td></td><td>string</td></td><td></td></td><td>(mongodb|mongodb+srv)://[<user>:<password>@]<host>[:<port>]/<database></td></td><td>high</td></td></tr>
<tr>
<td>mongo.collection.format</td></td><td>A format string for the destination collection name, which may contain `${topic}` as a placeholder for the originating topic name.
For example, `kafka_${topic}` for the topic `orders` will map to the collection name `kafka_orders`.</td></td><td>string</td></td><td>{$topic}</td></td><td>Non-empty string</td></td><td>medium</td></td></tr>
<tr>
<td>mongo.offset.collection</td></td><td>The mongo collection for storage the latest offset processed.</td></td><td>string</td></td><td>OFFSETS</td></td><td></td></td><td>medium</td></td></tr>
</tbody></table>

- A sample configuration may look as below.

    ```ini    
    # Kafka consumer configuration
    name=kafka-connector-mongodb-sink
    
    # Kafka connector configuration
    connector.class=org.radarcns.connect.mongodb.MongoDbSinkConnector
    tasks.max=1
    
    # Topics that will be consumed
    topics=avrotest
    # MongoDB server
    mongo.uri=mongodb://me:pass123@localhost/kafka-db
    
    # Collection name for putting data into the MongoDB database. The {$topic} token will be replaced
    # by the Kafka topic name.
    #mongo.collection.format={$topic}
    
    # Factory class to do the actual record conversion
    record.converter.class=org.radarcns.connect.mongodb.serialization.RecordConverterFactory
    ```
    
- Run the MongoDB-Sink-Connector in 
  - `standalone` mode
  
      ```shell
      connect-standalone /etc/schema-registry/connect-avro-standalone.properties ./sink.properties 
      ```
  - `distributed` mode
  
      ```shell
      connect-distributed /patht/cluster.properties ./sink.properties
      ```
- Stream sample data to configured `topics` in `sink.properties`. You may use, [rest-proxy](http://docs.confluent.io/4.0.0/kafka-rest/docs/intro.html#produce-and-consume-avro-messages) to do this easily.

    ```shell
    curl -X POST -H "Content-Type: application/vnd.kafka.avro.v2+json" \
          -H "Accept: application/vnd.kafka.v2+json" \
          --data '{"value_schema": "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "testUser"}}]}' \
          "http://localhost:8082/topics/avrotest"
          
    
    # You should get the following response:
      {"offsets":[{"partition":0,"offset":0,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":21}

    ```  
- If your all of your environment properties are configured properly, when you run the mongodb-sink-connector, you may see output as below.
 
    ```shell
    SLF4J: Class path contains multiple SLF4J bindings.
    SLF4J: Found binding in [jar:file:/usr/share/java/confluent-common/slf4j-log4j12-1.7.6.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: Found binding in [jar:file:/usr/share/java/kafka-serde-tools/slf4j-log4j12-1.7.6.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: Found binding in [jar:file:/usr/share/java/kafka-connect-hdfs/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: Found binding in [jar:file:/usr/share/java/kafka/slf4j-log4j12-1.7.21.jar!/org/slf4j/impl/StaticLoggerBinder.class]
    SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
    SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
    [2017-04-14 15:23:35,626] INFO StandaloneConfig values: 
            cluster = connect
            rest.advertised.host.name = null
            task.shutdown.graceful.timeout.ms = 5000
            rest.host.name = null
            rest.advertised.port = null
            bootstrap.servers = [localhost:9092]
            offset.flush.timeout.ms = 5000
            offset.flush.interval.ms = 60000
            rest.port = 8083
            internal.key.converter = class org.apache.kafka.connect.json.JsonConverter
            access.control.allow.methods = 
            access.control.allow.origin = 
            offset.storage.file.filename = /tmp/connect.offsets
            internal.value.converter = class org.apache.kafka.connect.json.JsonConverter
            value.converter = class io.confluent.connect.avro.AvroConverter
            key.converter = class io.confluent.connect.avro.AvroConverter
     (org.apache.kafka.connect.runtime.standalone.StandaloneConfig:178)
    [2017-04-14 15:23:35,767] INFO Logging initialized @509ms (org.eclipse.jetty.util.log:186)
    [2017-04-14 15:23:36,094] INFO Kafka Connect starting (org.apache.kafka.connect.runtime.Connect:52)
    [2017-04-14 15:23:36,094] INFO Herder starting (org.apache.kafka.connect.runtime.standalone.StandaloneHerder:71)
    [2017-04-14 15:23:36,094] INFO Worker starting (org.apache.kafka.connect.runtime.Worker:102)
    [2017-04-14 15:23:36,104] INFO ProducerConfig values: 
            metric.reporters = []
            metadata.max.age.ms = 300000
            ...
            ...
            partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
            linger.ms = 0
     (org.apache.kafka.clients.producer.ProducerConfig:178)
    [2017-04-14 15:23:36,138] INFO ProducerConfig values: 
            metric.reporters = []
            metadata.max.age.ms = 300000
            reconnect.backoff.ms = 50
            sasl.kerberos.ticket.renew.window.factor = 0.8
            bootstrap.servers = [localhost:9092]
            ...
            ...
            value.serializer = class org.apache.kafka.common.serialization.ByteArraySerializer
            ssl.keymanager.algorithm = SunX509
            metrics.sample.window.ms = 30000
            partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
            linger.ms = 0
     (org.apache.kafka.clients.producer.ProducerConfig:178)
    [2017-04-14 15:23:36,140] INFO Kafka version : 0.10.0.1-cp1 (org.apache.kafka.common.utils.AppInfoParser:83)
    [2017-04-14 15:23:36,141] INFO Kafka commitId : e7288edd541cee03 (org.apache.kafka.common.utils.AppInfoParser:84)
    [2017-04-14 15:23:36,141] INFO Starting FileOffsetBackingStore with file /tmp/connect.offsets (org.apache.kafka.connect.storage.FileOffsetBackingStore:60)
    [2017-04-14 15:23:36,143] INFO Worker started (org.apache.kafka.connect.runtime.Worker:124)
    [2017-04-14 15:23:36,144] INFO Herder started (org.apache.kafka.connect.runtime.standalone.StandaloneHerder:73)
    [2017-04-14 15:23:36,144] INFO Starting REST server (org.apache.kafka.connect.runtime.rest.RestServer:98)
    [2017-04-14 15:23:36,261] INFO jetty-9.2.15.v20160210 (org.eclipse.jetty.server.Server:327)
    [2017-04-14 15:23:36,931] INFO Started o.e.j.s.ServletContextHandler@d1f74b8{/,null,AVAILABLE} (org.eclipse.jetty.server.handler.ContextHandler:744)
    [2017-04-14 15:23:36,940] INFO Started ServerConnector@2ad3a1bb{HTTP/1.1}{0.0.0.0:8083} (org.eclipse.jetty.server.ServerConnector:266)
    [2017-04-14 15:23:36,941] INFO Started @1685ms (org.eclipse.jetty.server.Server:379)
    [2017-04-14 15:23:36,942] INFO REST server listening at http://127.0.1.1:8083/, advertising URL http://127.0.1.1:8083/ (org.apache.kafka.connect.runtime.rest.RestServer:150)
    [2017-04-14 15:23:36,942] INFO Kafka Connect started (org.apache.kafka.connect.runtime.Connect:58)
    [2017-04-14 15:23:36,945] INFO ConnectorConfig values: 
            connector.class = org.radarcns.connect.mongodb.MongoDbSinkConnector
            tasks.max = 1
            name = kafka-connector-mongodb-sink
     (org.apache.kafka.connect.runtime.ConnectorConfig:178)
    [2017-04-14 15:23:36,955] INFO Creating connector kafka-connector-mongodb-sink of type org.radarcns.connect.mongodb.MongoDbSinkConnector (org.apache.kafka.connect.runtime.Worker:168)
    [2017-04-14 15:23:36,957] INFO Instantiated connector kafka-connector-mongodb-sink with version 0.10.0.1-cp1 of type org.radarcns.connect.mongodb.MongoDbSinkConnector (org.apache.kafka.connect.runtime.Worker:176)
    [2017-04-14 15:23:36,959] INFO User configuration are: 
            mongo.port: 27017
            connector.class: org.radarcns.connect.mongodb.MongoDbSinkConnector
            mongo.password: ***
            mongo.database: mongodb-database
            record.converter.class: org.radarcns.connect.mongodb.serialization.RecordConverterFactory
            mongo.username: mongodb-username
            mongo.host: localhost
            topics: avrotest
            tasks.max: 1
            name: kafka-connector-mongodb-sink (org.radarcns.connect.mongodb.MongoDbSinkConnector:116)
    [2017-04-14 15:23:36,961] INFO Finished creating connector kafka-connector-mongodb-sink (org.apache.kafka.connect.runtime.Worker:181)
    [2017-04-14 15:23:36,961] INFO SinkConnectorConfig values: 
            connector.class = org.radarcns.connect.mongodb.MongoDbSinkConnector
            tasks.max = 1
            topics = [avrotest]
            name = kafka-connector-mongodb-sink
     (org.apache.kafka.connect.runtime.SinkConnectorConfig:178)
    [2017-04-14 15:23:36,963] INFO At most 1 will be started (org.radarcns.connect.mongodb.MongoDbSinkConnector:126)
    [2017-04-14 15:23:36,971] INFO TaskConfig values: 
            task.class = class org.radarcns.connect.mongodb.MongoDbSinkTask
     (org.apache.kafka.connect.runtime.TaskConfig:178)
    [2017-04-14 15:23:36,972] INFO Creating task kafka-connector-mongodb-sink-0 (org.apache.kafka.connect.runtime.Worker:315)
    [2017-04-14 15:23:36,972] INFO Instantiated task kafka-connector-mongodb-sink-0 with version 0.10.0.1-cp1 of type org.radarcns.connect.mongodb.MongoDbSinkTask (org.apache.kafka.connect.runtime.Worker:326)
    [2017-04-14 15:23:36,987] INFO ConsumerConfig values: 
            metric.reporters = []
            ...
            ...
            send.buffer.bytes = 131072
            value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
            group.id = connect-kafka-connector-mongodb-sink
            retry.backoff.ms = 100
            ...
            ...
            key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
            ...
            ...
     (org.apache.kafka.clients.consumer.ConsumerConfig:178)
    [2017-04-14 15:23:37,001] INFO ConsumerConfig values: 
            metric.reporters = []
            metadata.max.age.ms = 300000
            partition.assignment.strategy = [org.apache.kafka.clients.consumer.RangeAssignor]
            ...
            ...
            auto.offset.reset = earliest
     (org.apache.kafka.clients.consumer.ConsumerConfig:178)
    [2017-04-14 15:23:37,038] INFO Kafka version : 0.10.0.1-cp1 (org.apache.kafka.common.utils.AppInfoParser:83)
    [2017-04-14 15:23:37,039] INFO Kafka commitId : e7288edd541cee03 (org.apache.kafka.common.utils.AppInfoParser:84)
    [2017-04-14 15:23:37,041] INFO Created connector kafka-connector-mongodb-sink (org.apache.kafka.connect.cli.ConnectStandalone:91)
    [2017-04-14 15:23:37,042] INFO 0 have been processed (org.radarcns.connect.mongodb.MongoDbSinkTask:56)
    [2017-04-14 15:23:37,115] INFO Cluster created with settings {hosts=[localhost:27017], mode=SINGLE, requiredClusterType=UNKNOWN, serverSelectionTimeout='30000 ms', maxWaitQueueSize=500} (org.mongodb.driver.cluster:71)
    [2017-04-14 15:23:37,132] INFO 0 have been written in MongoDB 0 records need to be processed. (org.radarcns.connect.mongodb.MongoDbWriter:58)
    [2017-04-14 15:23:37,142] INFO No server chosen by ReadPreferenceServerSelector{readPreference=primary} from cluster description ClusterDescription{type=UNKNOWN, connectionMode=SINGLE, serverDescriptions=[ServerDescription{address=localhost:27017, type=UNKNOWN, state=CONNECTING}]}. Waiting for 30000 ms before timing out (org.mongodb.driver.cluster:71)
    [2017-04-14 15:23:37,413] INFO Opened connection [connectionId{localValue:1, serverValue:5}] to localhost:27017 (org.mongodb.driver.connection:71)
    [2017-04-14 15:23:37,415] INFO Monitor thread successfully connected to server with description ServerDescription{address=localhost:27017, type=STANDALONE, state=CONNECTED, ok=true, version=ServerVersion{versionList=[3, 2, 10]}, minWireVersion=0, maxWireVersion=4, maxDocumentSize=16777216, roundTripTimeNanos=722850} (org.mongodb.driver.cluster:71)
    [2017-04-14 15:23:37,510] INFO Opened connection [connectionId{localValue:2, serverValue:6}] to localhost:27017 (org.mongodb.driver.connection:71)
    [2017-04-14 15:23:37,534] INFO Sink task WorkerSinkTask{id=kafka-connector-mongodb-sink-0} finished initialization and start (org.apache.kafka.connect.runtime.WorkerSinkTask:208)
    [2017-04-14 15:23:37,534] INFO Started MongoDbWriter (org.radarcns.connect.mongodb.MongoDbWriter:97)
    [2017-04-14 15:23:37,687] INFO Discovered coordinator nivethika-XPS-15-9550:9092 (id: 2147483647 rack: null) for group connect-kafka-connector-mongodb-sink. (org.apache.kafka.clients.consumer.internals.AbstractCoordinator:528)
    [2017-04-14 15:23:37,688] INFO Revoking previously assigned partitions [] for group connect-kafka-connector-mongodb-sink (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:292)
    [2017-04-14 15:23:37,688] INFO (Re-)joining group connect-kafka-connector-mongodb-sink (org.apache.kafka.clients.consumer.internals.AbstractCoordinator:349)
    [2017-04-14 15:23:37,731] INFO Successfully joined group connect-kafka-connector-mongodb-sink with generation 1 (org.apache.kafka.clients.consumer.internals.AbstractCoordinator:457)
    [2017-04-14 15:23:37,732] INFO Setting newly assigned partitions [avrotest-0] for group connect-kafka-connector-mongodb-sink (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:231)
    [2017-04-14 15:23:42,197] INFO Reflections took 5994 ms to scan 254 urls, producing 12617 keys and 82584 values  (org.reflections.Reflections:229)
    [2017-04-14 15:24:07,043] INFO 4 have been processed (org.radarcns.connect.mongodb.MongoDbSinkTask:56)
    [2017-04-14 15:24:07,131] INFO 4 have been written in MongoDB 0 records need to be processed. (org.radarcns.connect.mongodb.MongoDbWriter:58)
    [2017-04-14 15:24:36,975] INFO [FLUSH-WRITER] Time-elapsed: 3.6946E-5 s (org.radarcns.connect.mongodb.MongoDbWriter:205)
    [2017-04-14 15:24:36,996] INFO [FLUSH] Time elapsed: 0.020898882 s (org.radarcns.connect.mongodb.MongoDbSinkTask:153)
    [2017-04-14 15:24:36,996] INFO WorkerSinkTask{id=kafka-connector-mongodb-sink-0} Committing offsets (org.apache.kafka.connect.runtime.WorkerSinkTask:261)
    [2017-04-14 15:24:37,043] INFO 2 have been processed (org.radarcns.connect.mongodb.MongoDbSinkTask:56)
    [2017-04-14 15:24:37,131] INFO 2 have been written in MongoDB 0 records need to be processed. (org.radarcns.connect.mongodb.MongoDbWriter:58)
    ```
- You can view the data in MongoDB. For the above example you may see results as follows.

    ```shell
    # log-in to mongo cli
    mongo
    MongoDB shell version: 3.2.12
    connecting to: test
    Server has startup warnings: 
    2017-04-14T13:14:29.177+0000 I CONTROL  [initandlisten] ** WARNING: You are running this process as the root user, which is not recommended.
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] 
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] 
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] ** WARNING: /sys/kernel/mm/transparent_hugepage/enabled is 'always'.
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] **        We suggest setting it to 'never'
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] 
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] ** WARNING: /sys/kernel/mm/transparent_hugepage/defrag is 'always'.
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] **        We suggest setting it to 'never'
    2017-04-14T13:14:29.178+0000 I CONTROL  [initandlisten] 
    
    # view available databases
    > show dbs
    admin       0.000GB
    mongodb-database  0.000GB
    local       0.000GB
    
    # switch to your database
    > use mongodb-database
    switched to db mongodb-database
    
    # view available collections. You should see collections with configured topic names and additional `OFFSETS` collection
    > show collections
    OFFSETS
    avrotest
    
    # query the data inside collections
    > db.avrotest.find()
    { "_id" : null, "name" : "somee" }
    > db.OFFSETS.find()
    { "_id" : { "topic" : "avrotest", "partition" : 0 }, "offset" : NumberLong(5) }
    > 
    ```
- To stop your connector press `CTRL-C`

## Developer guide 

This `MongoDB-Sink-Connector` works based on `RecordConverter`s to convert a `SinkRecord` to a `Document`. The default [RecordConverter](https://github.com/RADAR-base/MongoDb-Sink-Connector/blob/master/src/main/java/org/radarcns/connect/mongodb/serialization/RecordConverter.java) is [GenericRecordConverter](https://github.com/RADAR-base/MongoDb-Sink-Connector/blob/master/src/main/java/org/radarcns/serialization/GenericRecordConverter.java), which converts a record-key as `_id` and adds a field for every field-name from record-value. The `GenericRecordConverter` supports conversion of most of the primitive types and collections.

For Avro records with complex schemas, or for custom collection format it is recommended to write your own `RecordConverter` and register it to an extended `RecordConverterFactory`. Writing a custom `RecordConverter` is relatively straight forward. The interface requires two methods to be implemented.
```java
/**
 * Converts Kafka records to MongoDB documents.
 */
public interface RecordConverter {
    /**
     * <p>The schema names used are the fully qualified (including namespace) and case-sensitive
     * names. If the converter requires records with both a key and a value schema, the  returned
     * format is "KeySchemaName-ValueSchemaName". If the key is not required, only "ValueSchemaName"
     * may be returned. KeySchemaName and ValueSchemaName may be substituted by the Object class
     * that it supports. If the converter supports all types of data, return null.
     */
    Collection<String> supportedSchemaNames();

    /**
     * Convert a Kafka record to a BSON document.
     *
     * @param record record to convert
     * @return BSON document
     * @throws DataException if the record cannot be converted by the current converter.
     */
    Document convert(SinkRecord record) throws DataException;
}
```

### Sample RecordConverter Implementation

1. Implement a custom RecordConverter. An example is given below. Consider a record consisting `key-schema` 
    
    ```json
    {
      "namespace": "org.radarcns.key",
      "type": "record",
      "name": "MeasurementKey",
      "doc": "Measurement key in the RADAR-base project",
      "fields": [
        {"name": "userId", "type": "string", "doc": "user ID"},
        {"name": "sourceId", "type": "string", "doc": "device source ID"}
      ]
    }
    ```
    and a `value-schema` as below.

    ```json
    {
      "namespace": "org.radarcns.application",
      "type": "record",
      "name": "ApplicationRecordCounts",
      "doc": "Number of records cached or created.",
      "fields": [
        {"name": "time", "type": "double", "doc": "device timestamp in UTC (s)"},
        {"name": "timeReceived", "type": "double", "doc": "device receiver timestamp in UTC (s)"},
        {"name": "recordsCached", "type": "int", "doc": "number of records currently being cached", "default": -1},
        {"name": "recordsSent", "type": "int", "doc": "number of records sent since application start"},
        {"name": "recordsUnsent", "type": "int", "doc": "number of unsent records", "default": -1}
      ]
    }
    ```
    These samples would give us the `KeySchemaName` as `org.radarcns.key.MeasurementKey` and `ValueSchemaName` as `org.radarcns.application.ApplicationRecordCounts`. Lets call our custom `RecordConverter` as `CountsStatusRecordConverter`. The implementation can be as simple as below.
    ```java
    /**
     * RecordConverter to convert a StatusCounts record to a MongoDB Document.
     */
    public class CountsStatusRecordConverter implements RecordConverter {
    
        /**
         * Returns the list of supported schemas, which behaves as the id to select suitable
         * RecordConverter for a SinkRecord.
         *
         * @return a list of supported Schemas
         */
        @Override
        public Collection<String> supportedSchemaNames() {
            return Collections.singleton("org.radarcns.key.MeasurementKey" + "-"
                    + "org.radarcns.application.ApplicationRecordCounts");
        }
    
        /**
         * Converts a ServerStatus SinkRecord into a MongoDB Document.
         *
         * @param sinkRecord record to be converted
         * @return converted MongoDB Document to write
         */
        @Override
        public Document convert(SinkRecord sinkRecord) throws DataException {
    
            Struct key = (Struct) sinkRecord.key();
            Struct value = (Struct) sinkRecord.value();
    
            return new Document("_id", key.get("userId") + "-" + key.get("sourceId"))
                    .append("user", key.getString("userId"))
                    .append("source", key.getString("sourceId"))
                    .append("recordsCached", value.getInt32("recordsCached"))
                    .append("recordsSent", value.getInt32("recordsSent"))
                    .append("timestamp", Converter.toDateTime(value.get("timeReceived")));
        }
    }
    ```
2. Register implemented `RecordConverter` to an extended `RecordConverterFactory`.

    ```java
    package org.radarcns.connect.mongodb.example;
    /**
     * Extended RecordConverterFactory to allow customized RecordConverter class that are needed
     */
    public class RecordConverterFactoryExample extends RecordConverterFactory {
    
        /**
         * Overrides genericConverter to append custom RecordConverter class to RecordConverterFactory
         *
         * @return list of RecordConverters available
         */
        protected List<RecordConverter> genericConverters() {
            List<RecordConverter> recordConverters = new ArrayList<RecordConverter>();
            recordConverters.addAll(super.genericConverters());
            recordConverters.add(new CountsStatusRecordConverter());
            return recordConverters;
        }
    
    }
    ```
3. Use extended `RecordConverterFactoryExample` in `sink.properties`

    ```ini
    # Factory class to do the actual record conversion
    record.converter.class=org.radarcns.connect.mongodb.example.RecordConverterFactoryExample
    ```

#### Notes

The only available setting is the number of records returned in a single call to `poll()` (i.e. `consumer.max.poll.records` param inside `standalone.properties`)

Connectors can be run inside any machine where Kafka has been installed. Therefore, you can fire them also inside a machine that does not host a Kafka broker.

To reset a connector running in `standalone mode` you have to stop it and then modify `name` and `offset.storage.file.filename` respectively inside `sink.properties` and `standalone.properties`

## Javadoc
More info and Javadocs of the connector are available at - 
- [Javadoc](https://radar-base.github.io/MongoDb-Sink-Connector/)

## Contributing

All of the contribution code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-base/RADAR-MongoDB-Sink-Connector/issues), and please make a pull request.
