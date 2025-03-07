package com.example.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.kafka.clients.producer.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Masking_flex<K, V> implements ProducerInterceptor<K, V> {
    private List<String> columnsToMask = new ArrayList<>();
    private String maskedValue = "***"; // Default masked value
    private Schema schema; // Avro Schema
    private Producer<String, String> producer;


    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        if (record.value() == null) {
            return record;
        }

        System.out.println("Incoming record value type: " + record.value().getClass().getName());
        System.out.println("Incoming record value: " + record.value());

        V newValue = record.value();

        if (newValue instanceof String) {
            return handleStringValue(record);
        }
        else if (newValue instanceof byte[]) {
            byte[] byteArray = (byte[]) newValue;

            if (byteArray.length > 5 && byteArray[0] == 0x00) {
                return handleByteValue(record);
            } else {
                return handleRawByteValue(record);
            }
        }
        else if (newValue instanceof GenericRecord) {
            return handleGenericRecordValue(record);
        }
        else {
            System.out.println("Unsupported record type: " + record.value().getClass().getName());
            return record;
        }
    }

    private ProducerRecord<K, V> handleRawByteValue(ProducerRecord<K, V> record) {
        K newKey = record.key();
        byte[] byteArray = (byte[]) record.value();

        System.out.println("Processing raw byte[] as JSON String.");

        String value = new String(byteArray, StandardCharsets.UTF_8);
        System.out.println("Original raw byte[] as String: " + value);

        for (String column : columnsToMask) {
            value = maskColumn(value, column.trim(), maskedValue);
        }

        System.out.println("Masked JSON String value: " + value);

        byte[] maskedValueBytes = value.getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), newKey, (V) maskedValueBytes, record.headers());
    }


    private ProducerRecord<K, V> handleStringValue(ProducerRecord<K, V> record) {
        K newKey = record.key();
        String value = (String) record.value();
        System.out.println("Original String value : " + value);
        value = applyMasking(value);
        System.out.println("Masked String value: " + value);
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                newKey, (V) value, record.headers());
    }
    private byte[] serializeAvro(GenericRecord record, byte[] originalAvroData) {
        try {
            if (record == null) {
                System.err.println(" Attempted to serialize a null Avro record.");
                return new byte[0];
            }

            // Serialize Avro record
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
            datumWriter.write(record, encoder);
            encoder.flush();
            byte[] avroBytes = outputStream.toByteArray();

            ByteArrayInputStream inputStream = new ByteArrayInputStream(originalAvroData);
            byte[] schemaHeader = new byte[5]; // Magic Byte + Schema ID (4 byte)
            inputStream.read(schemaHeader, 0, 5);

            ByteArrayOutputStream finalOutput = new ByteArrayOutputStream();
            finalOutput.write(schemaHeader); // Magic Byte + Schema ID
            finalOutput.write(avroBytes); // Dữ liệu Avro mới

            byte[] finalAvroBytes = finalOutput.toByteArray();
            System.out.println(" Serialized Avro Data with Header (hex): " + bytesToHex(finalAvroBytes));

            return finalAvroBytes;

        } catch (IOException e) {
            System.err.println(" Lỗi khi serialize Avro: " + e.getMessage());
            return new byte[0];
        }
    }


    private GenericRecord deserializeAvro(byte[] avroData) {
        try {
            if (avroData == null || avroData.length == 0) {
                System.err.println(" Avro data is null or empty.");
                return null;
            }

            if (schema == null) {
                System.err.println(" Schema is not available. Cannot deserialize Avro.");
                return null;
            }

            System.out.println("Attempting to decode Avro data...");
            System.out.println("Avro data length: " + avroData.length);


            ByteArrayInputStream inputStream = new ByteArrayInputStream(avroData);
            inputStream.skip(5);

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

            GenericRecord record = datumReader.read(null, decoder);

            System.out.println(" Decoded Avro Record Schema: " + record.getSchema().toString());
            System.out.println(" Decoded Avro Record (before masking): " + record);

            return record;

        } catch (IOException e) {
            System.err.println(" Lỗi khi giải mã Avro: " + e.getMessage());
            return null;
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }


    private ProducerRecord<K, V> handleByteValue(ProducerRecord<K, V> record) {
        K newKey = record.key();
        byte[] byteArray = (byte[]) record.value();
        System.out.println("Incoming Avro byte array length: " + byteArray.length);
        System.out.println("Raw Avro byte data (hex): " + bytesToHex(byteArray));

        if (byteArray.length == 0) {
            System.err.println(" Received empty byte array.");
            return record;
        }

        if (schema == null) {
            System.err.println(" Schema is not defined. Skipping masking.");
            return record;
        }
        try {
            GenericRecord avroRecord = deserializeAvro(byteArray);
            if (avroRecord == null) {
                System.err.println(" Failed to deserialize Avro. Skipping masking.");
                return record;
            }

            System.out.println(" Before Masking: " + avroRecord);
            for (String column : columnsToMask) {
                if (avroRecord.hasField(column.trim()) && avroRecord.get(column.trim()) != null) {
                    avroRecord.put(column.trim(), maskedValue);
                    System.out.println("Masked field: " + column.trim());
                }
            }
            System.out.println("After Masking: " + avroRecord);

            byte[] maskedValueBytes = serializeAvro(avroRecord, byteArray);
            System.out.println(" Sending masked Avro record");

            return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                     newKey, (V) maskedValueBytes, record.headers());

        } catch (Exception e) {
            System.err.println(" Lỗi khi xử lý Avro: " + e.getMessage());
            return record;
        }
    }




    private ProducerRecord<K, V> handleGenericRecordValue(ProducerRecord<K, V> record) {
        K newKey = record.key();
        GenericRecord value = (GenericRecord) record.value();
        System.out.println("Original GenericRecord value: " + value);

        for (String column : columnsToMask) {
            if (value.get(column) != null) {
                value.put(column, maskedValue);
            }
        }

        System.out.println("Masked GenericRecord value: " + value);
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                newKey, (V) value, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            System.out.println("Record sent to partition: " + metadata.partition() + " with offset: " + metadata.offset());
        } else if (exception != null) {
            System.err.println("Error while producing record: " + exception.getMessage());
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
    @Override
    public void configure(Map<String, ?> configs) {
        String columnsConfig = (String) configs.get("columns.to.mask");
        if (columnsConfig != null) {
            columnsToMask = List.of(columnsConfig.replace("[", "").replace("]", "").replace("\"", "").trim().split(","));
        }
        Object replacementValue = configs.get("replacement.value");
        if (replacementValue != null) {
            maskedValue = (String) replacementValue;
        } else {
            maskedValue = "***"; // Default value
        }
        String schemaJson = (String) configs.get("value.schema");
        if (schemaJson != null && !schemaJson.isEmpty()) {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(schemaJson);
            System.out.println("Loaded Schema: " + schema.toString(true));

        } else {
            System.err.println("Schema configuration is missing!");
        }
//        String schemaJson = (String) configs.get("value.schema");
//        Schema.Parser parser = new Schema.Parser();
//        schema = parser.parse(schemaJson);

    }
//    private Map<String, Object> buildProducerConfigs(Map<String, ?> configs) {
//        Map<String, Object> copyConfigs = new HashMap<>(configs);
//        copyConfigs.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
//        copyConfigs.put(ProducerConfig.RETRIES_CONFIG, "0");
//        copyConfigs.put(ProducerConfig.ACKS_CONFIG, "1");
//        copyConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "0");
//        copyConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        copyConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
//        copyConfigs.put("schema.registry.url","http://192.168.1.5:8081");
//        return copyConfigs;
//    }

    private String applyMasking(String json) {
        for (String column : columnsToMask) {
            json = maskColumn(json, column.trim(), maskedValue);
        }
        return json;
    }

    private String maskColumn(String json, String column, String maskedValue) {
        String regex = "(?i)\"\\s*" + column + "\\s*\"\\s*:\\s*(\"[^\"]*\"|\\d+(\\.\\d+)?|true|false|null)";
        String replacement = "\"" + column + "\":\"" + maskedValue + "\"";
        return json.replaceAll(regex, replacement);
    }
}
