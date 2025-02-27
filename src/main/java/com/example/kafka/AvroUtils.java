package com.example.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.EncoderFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroUtils {
    public static String convertRecordToJson(GenericRecord record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SpecificDatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);

        writer.write(record, encoder);
        encoder.flush();
        return new String(outputStream.toByteArray(), "UTF-8");
    }
}
