package com.example.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) {
        // Define Kafka properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Add the custom interceptor to mask data
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Masking_flex.class.getName());

        // Include the Avro schema in the configuration
        String schemaString = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"User\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"age\", \"type\": \"int\"}\n" +
                "  ]\n" +
                "}";
        properties.put("avro.schema", schemaString);

        // Create Kafka producer
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        // Create an Avro record
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        GenericRecord record = new org.apache.avro.generic.GenericData.Record(schema);
        record.put("name", "John Doe");
        record.put("age", 30);

        try {
            byte[] avroData = serializeAvroRecord(record, schema);

            ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>("test-topic_v5", "key1", avroData);

            // Send the record to Kafka
            producer.send(kafkaRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
                    }
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    // Helper method to serialize Avro record to byte array
    private static byte[] serializeAvroRecord(GenericRecord record, Schema schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write(record, encoder);
        encoder.flush();
        return outputStream.toByteArray();
    }
}
