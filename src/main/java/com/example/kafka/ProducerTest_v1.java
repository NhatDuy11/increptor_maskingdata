package com.example.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest_v1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.example.kafka.Masking_flex_v1");
        properties.put("columns.to.mask", "field4,field1");
        properties.put("replacement.value.field4", 6666);
        properties.put("replacement.value.field1", "Duy");
        properties.put("schema.registry.url", "http://192.168.1.5:8081");
        String schemaString = "{\"type\":\"record\",\"name\":\"ExampleRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"IP\",\"type\":\"string\"},{\"name\":\"field3\",\"type\":\"string\"},{\"name\":\"field4\",\"type\":\"int\"}]}";
        properties.put("value.schema",schemaString);


        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        Schema schema = new Schema.Parser().parse(schemaString);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("field1", "value1");
        avroRecord.put("IP", "192.168.1.5");
        avroRecord.put("field3", "OtherData");
        avroRecord.put("field4", 2222);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("test-topic_avro_ogg_v1", "key1", avroRecord);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.println("Record sent to topic: " + metadata.topic() +
                        " partition: " + metadata.partition() +
                        " offset: " + metadata.offset());
            }
        });

        producer.flush();
        producer.close();
    }

}
