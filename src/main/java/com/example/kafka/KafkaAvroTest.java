package com.example.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class KafkaAvroTest {
    public static void main(String[] args) throws IOException {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"ExampleRecord\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\": \"field1\", \"type\": \"string\" },\n" +
                "    { \"name\": \"IP\", \"type\": \"string\" },\n" +
                "    { \"name\": \"field3\", \"type\": \"string\" },\n" +
                "    { \"name\": \"field4\", \"type\": \"string\" }\n" +
                "  ]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Create a GenericRecord
        GenericRecord record = new GenericData.Record(schema);
        record.put("field1", "value1");
        record.put("IP", "192.168.1.5");
        record.put("field3", "OtherData");
        record.put("field4", "32322323232");

        // Encode record to byte array
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        byte[] byteArray = out.toByteArray();

        // Decode byte array back to GenericRecord
        ByteArrayInputStream in = new ByteArrayInputStream(byteArray);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        GenericRecord decodedRecord = reader.read(null, decoder);

        // Print original and decoded record
        System.out.println("Original record: " + record);
        System.out.println("Decoded record: " + decodedRecord);
    }
}
