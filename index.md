## Installation

This connector can be used inside a Docker stack or installed as a general Kafka Connect plugin.



## Developer guide

This `MongoDB-Sink-Connector` works based on `RecordConverter`s to convert a `SinkRecord` to a `Document`. The default [RecordConverter](https://github.com/RADAR-CNS/MongoDb-Sink-Connector/blob/master/src/main/java/org/radarcns/connect/mongodb/serialization/RecordConverter.java) is [GenericRecordConverter](https://github.com/RADAR-CNS/MongoDb-Sink-Connector/blob/master/src/main/java/org/radarcns/serialization/GenericRecordConverter.java), which converts a record-key as `_id` and adds a field for every field-name from record-value. The `GenericRecordConverter` supports conversion of most of the primitive types and collections.

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

## Info
For more details on configuring & installing MongoDb Sink Connector, refer to the readme on our [Github repository](https://github.com/RADAR-base/MongoDb-Sink-Connector).

- [Javadoc](https://radar-base.github.io/MongoDb-Sink-Connector/javadoc/)

