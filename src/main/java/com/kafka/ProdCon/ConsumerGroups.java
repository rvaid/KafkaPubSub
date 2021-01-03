package com.kafka.ProdCon;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
        Properties properties = new Properties();
        String topic = "topic-01";

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-3");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        // Subscribe consumer to our topic
        consumer.subscribe(Arrays.asList(topic));

        // Poll for new data
        while (true){
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : consumerRecords){
                logger.info("Key: "+ record.key());
                logger.info("value: "+ record.value());
                logger.info("Partition: "+ record.partition());
            }
        }
    }
}
