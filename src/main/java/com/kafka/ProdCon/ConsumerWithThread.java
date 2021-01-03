package com.kafka.ProdCon;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {
   public void run(){
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        String topic = "topic-01";
        String bootStrapServers = "localhost:9092";
        String groupId = "group-4";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(
                bootStrapServers,
                groupId,
                topic,
                latch
        );

        // start the thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        // add shutdown hook
       Runtime.getRuntime().addShutdownHook(new Thread(()->{
           logger.info("Caught shutdown hook");
           ((ConsumerRunnable)consumerRunnable).shutdown();
           try {
               latch.await();
           } catch (InterruptedException e) {
                   e.printStackTrace();
           }
           logger.info("Application has exited");
       }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }
   }
    public static void main(String[] args) {
       new ConsumerWithThread().run();
    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        public ConsumerRunnable(String bootStrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch){
            Properties properties = new Properties();

            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.latch = latch;
            this.consumer = new KafkaConsumer<String, String>(properties);
            this.consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while (true){
                    ConsumerRecords<String, String> consumerRecords =
                            consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : consumerRecords){
                        logger.info("Key: "+ record.key());
                        logger.info("value: "+ record.value());
                        logger.info("Partition: "+ record.partition());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal");
            }finally {
                consumer.close();
                // tell the main code, we are done with the consumer
                latch.countDown();
            }

        }

        public void shutdown(){
            // special method to interrupt consumer.poll
            // It will throw WakeUpException
            consumer.wakeup();
        }
    }
}
