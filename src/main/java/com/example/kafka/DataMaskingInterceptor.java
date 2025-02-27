package com.example.kafka;
import org.apache.kafka.clients.producer.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
public class DataMaskingInterceptor<K, V> implements ProducerInterceptor<K, V> {
    private List<String> columnsToMask;
    private String maskedValue;
    private Producer<String, String> producer;
    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        K newKey = record.key();
        V newValue = record.value();

        if (newValue instanceof byte[]) {
            String value = new String((byte[]) newValue, StandardCharsets.UTF_8);
            for (String column : columnsToMask) {
                value = maskColumn(value, column.trim(), maskedValue);
            }
            System.out.println("Masked byte[] value_new: " + value);
            newValue = (V) value.getBytes(StandardCharsets.UTF_8);
        } else if (newValue instanceof String) {
            String value = (String) newValue;
            System.out.println("Original String value: " + value);
            for (String column : columnsToMask) {
                value = maskColumn(value, column.trim(), maskedValue);
            }
            System.out.println("Masked String value: " + value);
            newValue = (V) value;
        }

        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), newKey, newValue, record.headers());
    }
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            System.out.println("Record sent to partition: " + metadata.partition() + " with offset: " + metadata.offset());
        } else {
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
        columnsConfig = columnsConfig.replace("[", "").replace("]", "").replace("\"", "").trim();
        this.columnsToMask = List.of(columnsConfig.split(","));
        System.out.println("Columns to mask: " + this.columnsToMask);
        if (configs.containsKey("replacement.value")) {
            this.maskedValue = (String) configs.get("replacement.value");
        }
    }
    private String maskColumn(String json, String column, String maskedValue) {
        System.out.println("Before masking column " + column + ": " + json);

        String regex = "(?i)\"\\s*" + column + "\\s*\"\\s*:\\s*(\"[^\"]*\"|\\d+(\\.\\d+)?|true|false|null)";
        String replacement = "\"" + column + "\":\"" + maskedValue + "\"";

        String maskedJson = json.replaceAll(regex, replacement);

        if (maskedJson.equals(json)) {
            System.err.println(" Warning: Field \"" + column + "\" không tồn tại trong JSON.");
        } else {
            System.out.println("After masking column " + column + ": " + maskedJson);
        }

        return maskedJson;
    }
}
