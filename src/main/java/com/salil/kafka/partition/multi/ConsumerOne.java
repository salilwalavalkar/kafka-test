package com.salil.kafka.partition.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class ConsumerOne {

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
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 0
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 2
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 5
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 6
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 10
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 11
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 12
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 15
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 17
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 18
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 20
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 21
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 23
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 26
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 27
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 34
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 36
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 37
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 38
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 41
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 42
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 43
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 45
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 47
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 48
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 49
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 51
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 52
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 54
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 55
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 56
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 58
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 61
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 62
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 63
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 67
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 68
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 69
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 72
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 74
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 76
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 82
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 83
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 85
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 86
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 89
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 91
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 93
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 94
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 95
 * Topic - kafka-partitions-test-101, Partition - 0, Value: Test message - 98
 */


