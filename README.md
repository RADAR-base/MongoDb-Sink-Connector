# MongoDb-Sink-Connector

[![Build Status](https://travis-ci.org/RADAR-CNS/MongoDb-Sink-Connector.svg?branch=master)](https://travis-ci.org/RADAR-CNS/MongoDb-Sink-Connector)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/64eb2330ca7146fcb0b823816f44fcb8)](https://www.codacy.com/app/RADAR-CNS/RADAR-MongoDbConnector?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-MongoDbConnector&amp;utm_campaign=Badge_Grade)

The MongoDB-Sink-Connector is a Kafka-Connector for scalably and reliably streaming data from a Kafka topic or number of Kafka topics to a MongoDB collection or number of MongoDB collections.
It consumes Avro data from Kafka topics, converts them into Documents and inserts them into MongoDB collections.
 
Currently, it supports records that have a value schema.

## Prerequisites

- Confluent platform installed
- MongoDB instance installed and running with access credentials
- MongoDB-Sink-Connector executable


## Quickstart for MongoDb-Sink-Connector
- Build MongoDB-Sink-Connector
    ```shell
    git clone https://github.com/RADAR-CNS/MongoDb-Sink-Connector.git
    
    cd MongoDb-Sink-Connector
    
    ./gradlew clean build
    ```
    or you can also download the latest release from [here](https://github.com/RADAR-CNS/MongoDb-Sink-Connector/releases).
- Follow [Confluent platform Quick start ](http://docs.confluent.io/3.2.0/quickstart.html)to start zookeeper, kafka-server, schema-registry and kafka-rest to easily stream data to a Kafka topic
    ```ini
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

- Export kafka-connect-mongodb-sink-*.jar to `CLASSPATH`
    ```ini
    export CLASSPATH=/pathto/kafka-connect-mongodb-sink-*.jar
    ```
- Modify [sink.properties](https://github.com/RADAR-CNS/MongoDb-Sink-Connector/blob/master/sink.properties) file according your environment. The following properties are supported:

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
<td>mongo.database</td><td>MongoDB database name</td><td>string</td><td></td><td></td><td>high</td></tr>
<tr>
<td>mongo.host</td><td>MongoDB host name to write data to</td><td>string</td><td></td><td></td><td>high</td></tr>
<tr>
<td>topics</td><td>List of topics to be streamed.</td><td>list</td><td></td><td></td><td>high</td></tr>
<tr>
<td>collection.format</td><td>A format string for the destination collection name, which may contain `${topic}`as a placeholder for the originating topic name.
For example, `kafka_${topic}` for the topic `orders` will map to the collection name `kafka_orders`.</td><td>string</td><td>{$topic}</td><td></td><td>medium</td></tr>
<tr>
<td>mongo.password</td><td>Password to connect to MongoDB database. If not set, no credentials are used.</td><td>string</td><td>null</td><td></td><td>medium</td></tr>
<tr>
<td>mongo.username</td><td>Username to connect to MongoDB database. If not set, no credentials are used.</td><td>string</td><td>null</td><td></td><td>medium</td></tr>
<tr>
<td>record.converter.class</td><td>RecordConverterFactory that returns classes to convert Kafka SinkRecords to BSON documents.</td><td>class</td><td>class org.radarcns.serialization.RecordConverterFactory</td><td></td><td>medium</td></tr>
<tr>
<td>buffer.capacity</td><td>Maximum number of items in a MongoDB writer buffer. Once the buffer becomes full,the task fails.</td><td>int</td><td>20000</td><td>[1,...]</td><td>low</td></tr>
<tr>
<td>mongo.port</td><td>MongoDB port</td><td>int</td><td>27017</td><td>[1,...]</td><td>low</td></tr>
</tbody></table>

- A sample configuration may look as below.
    ```ini
    
    # Kafka consumer configuration
    name=kafka-connector-mongodb-sink
    
    # Kafka connector configuration
    connector.class=org.radarcns.mongodb.MongoDbSinkConnector
    tasks.max=1
    
    # Topics that will be consumed
    topics=avrotest
    # MongoDB server
    mongo.host=localhost
    mongo.port=27017
    
    # MongoDB configuration
    mongo.username=mongodb-username
    mongo.password=***
    mongo.database=mongodb-database
    
    # Collection name for putting data into the MongoDB database. The {$topic} token will be replaced
    # by the Kafka topic name.
    #mongo.collection.format={$topic}
    
    # Factory class to do the actual record conversion
    record.converter.class=org.radarcns.serialization.RecordConverterFactory
    ```
    
- Run the MongoDB-Sink-Connector in 
  - `standalone` mode
    ```shell
    connect-standalone /etc/schema-registry/connect-avro-standalone.properties ./sink.properties 
    ```
  - `distributed` mode
    ```shell
    connect-distributed cluster.properties sink.properties
    ```
- Stream sample data to configured `topics` in `sink.properties`. You may use, [rest-proxy](http://docs.confluent.io/3.2.0/kafka-rest/docs/intro.html#produce-and-consume-avro-messages) to do this easily.
    ```shell
    connect-distributed cluster.properties sink.properties
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
            connector.class = org.radarcns.mongodb.MongoDbSinkConnector
            tasks.max = 1
            name = kafka-connector-mongodb-sink
     (org.apache.kafka.connect.runtime.ConnectorConfig:178)
    [2017-04-14 15:23:36,955] INFO Creating connector kafka-connector-mongodb-sink of type org.radarcns.mongodb.MongoDbSinkConnector (org.apache.kafka.connect.runtime.Worker:168)
    [2017-04-14 15:23:36,957] INFO Instantiated connector kafka-connector-mongodb-sink with version 0.10.0.1-cp1 of type org.radarcns.mongodb.MongoDbSinkConnector (org.apache.kafka.connect.runtime.Worker:176)
    [2017-04-14 15:23:36,959] INFO User configuration are: 
            mongo.port: 27017
            connector.class: org.radarcns.mongodb.MongoDbSinkConnector
            mongo.password: ***
            mongo.database: mongodb-database
            record.converter.class: org.radarcns.serialization.RecordConverterFactory
            mongo.username: mongodb-username
            mongo.host: localhost
            topics: avrotest
            tasks.max: 1
            name: kafka-connector-mongodb-sink (org.radarcns.mongodb.MongoDbSinkConnector:116)
    [2017-04-14 15:23:36,961] INFO Finished creating connector kafka-connector-mongodb-sink (org.apache.kafka.connect.runtime.Worker:181)
    [2017-04-14 15:23:36,961] INFO SinkConnectorConfig values: 
            connector.class = org.radarcns.mongodb.MongoDbSinkConnector
            tasks.max = 1
            topics = [avrotest]
            name = kafka-connector-mongodb-sink
     (org.apache.kafka.connect.runtime.SinkConnectorConfig:178)
    [2017-04-14 15:23:36,963] INFO At most 1 will be started (org.radarcns.mongodb.MongoDbSinkConnector:126)
    [2017-04-14 15:23:36,971] INFO TaskConfig values: 
            task.class = class org.radarcns.mongodb.MongoDbSinkTask
     (org.apache.kafka.connect.runtime.TaskConfig:178)
    [2017-04-14 15:23:36,972] INFO Creating task kafka-connector-mongodb-sink-0 (org.apache.kafka.connect.runtime.Worker:315)
    [2017-04-14 15:23:36,972] INFO Instantiated task kafka-connector-mongodb-sink-0 with version 0.10.0.1-cp1 of type org.radarcns.mongodb.MongoDbSinkTask (org.apache.kafka.connect.runtime.Worker:326)
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
    [2017-04-14 15:23:37,042] INFO 0 have been processed (org.radarcns.mongodb.MongoDbSinkTask:56)
    [2017-04-14 15:23:37,115] INFO Cluster created with settings {hosts=[localhost:27017], mode=SINGLE, requiredClusterType=UNKNOWN, serverSelectionTimeout='30000 ms', maxWaitQueueSize=500} (org.mongodb.driver.cluster:71)
    [2017-04-14 15:23:37,132] INFO 0 have been written in MongoDB 0 records need to be processed. (org.radarcns.mongodb.MongoDbWriter:58)
    [2017-04-14 15:23:37,142] INFO No server chosen by ReadPreferenceServerSelector{readPreference=primary} from cluster description ClusterDescription{type=UNKNOWN, connectionMode=SINGLE, serverDescriptions=[ServerDescription{address=localhost:27017, type=UNKNOWN, state=CONNECTING}]}. Waiting for 30000 ms before timing out (org.mongodb.driver.cluster:71)
    [2017-04-14 15:23:37,413] INFO Opened connection [connectionId{localValue:1, serverValue:5}] to localhost:27017 (org.mongodb.driver.connection:71)
    [2017-04-14 15:23:37,415] INFO Monitor thread successfully connected to server with description ServerDescription{address=localhost:27017, type=STANDALONE, state=CONNECTED, ok=true, version=ServerVersion{versionList=[3, 2, 10]}, minWireVersion=0, maxWireVersion=4, maxDocumentSize=16777216, roundTripTimeNanos=722850} (org.mongodb.driver.cluster:71)
    [2017-04-14 15:23:37,510] INFO Opened connection [connectionId{localValue:2, serverValue:6}] to localhost:27017 (org.mongodb.driver.connection:71)
    [2017-04-14 15:23:37,534] INFO Sink task WorkerSinkTask{id=kafka-connector-mongodb-sink-0} finished initialization and start (org.apache.kafka.connect.runtime.WorkerSinkTask:208)
    [2017-04-14 15:23:37,534] INFO Started MongoDbWriter (org.radarcns.mongodb.MongoDbWriter:97)
    [2017-04-14 15:23:37,687] INFO Discovered coordinator nivethika-XPS-15-9550:9092 (id: 2147483647 rack: null) for group connect-kafka-connector-mongodb-sink. (org.apache.kafka.clients.consumer.internals.AbstractCoordinator:528)
    [2017-04-14 15:23:37,688] INFO Revoking previously assigned partitions [] for group connect-kafka-connector-mongodb-sink (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:292)
    [2017-04-14 15:23:37,688] INFO (Re-)joining group connect-kafka-connector-mongodb-sink (org.apache.kafka.clients.consumer.internals.AbstractCoordinator:349)
    [2017-04-14 15:23:37,731] INFO Successfully joined group connect-kafka-connector-mongodb-sink with generation 1 (org.apache.kafka.clients.consumer.internals.AbstractCoordinator:457)
    [2017-04-14 15:23:37,732] INFO Setting newly assigned partitions [avrotest-0] for group connect-kafka-connector-mongodb-sink (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator:231)
    [2017-04-14 15:23:42,197] INFO Reflections took 5994 ms to scan 254 urls, producing 12617 keys and 82584 values  (org.reflections.Reflections:229)
    [2017-04-14 15:24:07,043] INFO 4 have been processed (org.radarcns.mongodb.MongoDbSinkTask:56)
    [2017-04-14 15:24:07,131] INFO 4 have been written in MongoDB 0 records need to be processed. (org.radarcns.mongodb.MongoDbWriter:58)
    [2017-04-14 15:24:36,975] INFO [FLUSH-WRITER] Time-elapsed: 3.6946E-5 s (org.radarcns.mongodb.MongoDbWriter:205)
    [2017-04-14 15:24:36,996] INFO [FLUSH] Time elapsed: 0.020898882 s (org.radarcns.mongodb.MongoDbSinkTask:153)
    [2017-04-14 15:24:36,996] INFO WorkerSinkTask{id=kafka-connector-mongodb-sink-0} Committing offsets (org.apache.kafka.connect.runtime.WorkerSinkTask:261)
    [2017-04-14 15:24:37,043] INFO 2 have been processed (org.radarcns.mongodb.MongoDbSinkTask:56)
    [2017-04-14 15:24:37,131] INFO 2 have been written in MongoDB 0 records need to be processed. (org.radarcns.mongodb.MongoDbWriter:58)
    ```
- To stop your connector press `CTRL-C`

To use further data types, extend `org.radarcns.serialization.RecordConverterFactory` and set the new class name in the `record.converter.class` property.
 
### Tuning
The only available setting is the number of records returned in a single call to `poll()` (i.e. `consumer.max.poll.records` param inside `standalone.properties`)

### Note
Connectors can be run inside any machine where Kafka has been installed. Therefore, you can fire them also inside a machine that does not host a Kafka broker.

## Reset
To reset a connector running in `standalone mode` you have to stop it and then modify `name` and `offset.storage.file.filename` respectively inside `sink.properties` and `standalone.properties`
