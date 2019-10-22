package com.atguigu.gmall.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaSender {

    public static KafkaProducer<String, String> kafkaProducer = null;

    public static void send(String topic, String msg) {

        if (kafkaProducer == null) {
            kafkaProducer = creatKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg));
    }

    private static KafkaProducer<String, String> creatKafkaProducer() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "gch102:9092,gch103:9092,gch104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }
}
