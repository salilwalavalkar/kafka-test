package com.salil.kafka.partition.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class ConsumerTwo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group-common-partition");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("kafka-partitions-test-101");
        kafkaConsumer.subscribe(topics);
        try{
            while (true){
                ConsumerRecords records = kafkaConsumer.poll(10);
                for(Object record : records) {
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", (( ConsumerRecord)record).topic(), (( ConsumerRecord)record).partition(), (( ConsumerRecord)record).value()));
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}

/**
 * Sample Output:
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 1
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 3
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 4
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 7
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 8
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 9
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 13
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 14
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 16
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 19
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 22
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 24
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 25
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 28
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 29
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 30
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 31
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 32
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 33
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 35
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 39
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 40
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 44
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 46
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 50
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 53
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 57
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 59
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 60
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 64
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 65
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 66
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 70
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 71
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 73
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 75
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 77
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 78
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 79
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 80
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 81
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 84
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 87
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 88
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 90
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 92
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 96
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 97
 * Topic - kafka-partitions-test-101, Partition - 1, Value: Test message - 99
 */

