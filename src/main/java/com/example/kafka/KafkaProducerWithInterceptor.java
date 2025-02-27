package com.example.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerWithInterceptor {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.5:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("interceptor.classes", "com.example.kafka.DataMaskingInterceptor");
        props.put("columns.to.mask", "F1");
        props.put("replacement.value", "******");


        try (FileOutputStream fos = new FileOutputStream("kafka-producer-config.properties")) {
            props.store(fos, "Kafka Producer Configuration with Data Masking Interceptor");
            System.out.println("File kafka-producer-config.properties đã được tạo thành công!");
        } catch (IOException e) {
            System.err.println("Không thể ghi file kafka-producer-config.properties: " + e.getMessage());
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String jsonMessage = "\n" +
                    "{\"after\":{\"F1\":\"E7TR0\",\"F2\":\"ybrqJNcJ\"}}";
            ProducerRecord<String, String> record = new ProducerRecord<>("ogg_masking", "E7TR0_ybrqJNcJ", jsonMessage);
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
