package com.tw.codher.shipment.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Service
public class OrderCreatedEventConsumer {

    public static Logger logger = LoggerFactory.getLogger(OrderCreatedEventConsumer.class);

    @Value("${order.created.topic}")
    String topicName;

    @Value("${broker.url}")
    String brokerUrl;

    @Value("${consumer.group.id}")
    String consumerGroupId;

    @Value("${polling.duration.timeout}")
    int pollingDurationTimeout;

    public void consume() {
        Map<String, String> consumerProperties = new HashMap<>();

        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer(consumerProperties);

        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(pollingDurationTimeout));
                System.out.println("Shipment Service polling for topic : "+topicName);
                for (ConsumerRecord<Integer, String> record : records) {
                    System.out.println("Key" + " :: " + record.key()+ ", Value" + " :: " + record.value());
                    System.out.println("Offset" + " :: " + record.offset()+ ", Partition" + " :: " + record.partition());
                }
            }
        } finally {
            consumer.close();
        }

    }
}
