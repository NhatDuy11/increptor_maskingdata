package com.example.kafka;

import org.apache.kafka.clients.producer.*;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.5:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("acks", "all");

        props.put("interceptor.classes", "com.example.kafka.Masking_flex");
        props.put("columns.to.mask", "F1");
        props.put("replacement.value", "******");

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {

            byte[] keyBytes = "E7TR0_ybrqJNcJ".getBytes(StandardCharsets.UTF_8);


            String jsonMessage = "{\"after\":{\"F1\":\"E7TR0\",\"F2\":\"ybrdqJNcJ\"}}";
            byte[] valueBytes = jsonMessage.getBytes(StandardCharsets.UTF_8);


            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("test_masking_byte", keyBytes, valueBytes);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Lỗi khi gửi dữ liệu: " + exception.getMessage());
                } else {
                    System.out.println("Sent message to topic " + metadata.topic() +
                            " partition " + metadata.partition() +
                            " at offset " + metadata.offset());
                }
            });
        } catch (Exception e) {
            System.err.println("Lỗi khi tạo Kafka Producer hoặc gửi dữ liệu: " + e.getMessage());
        }
    }
}
