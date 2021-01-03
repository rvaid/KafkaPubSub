package com.kafka.ProdCon;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        final Logger logger = LoggerFactory.getLogger(Producer.class);

        // Create producer properties
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a producer
        KafkaProducer<String, String> kafkaProducer =  new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){
            // Create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("topic-01", "kafka data " + Integer.toString(i));
            // Send producer record to the topic - happens asynchronously
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        logger.info("Received new metadata: \n" +
                                "Topic: "+metadata.topic() +
                                "\nPartition: "+metadata.partition() +
                                "\nOffset: "+metadata.offset() +
                                "\nTimestamp: "+metadata.timestamp()
                        );
                    }
                    else{
                        logger.error(exception.toString());
                    }
                }
            });
        }



        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
