package com.zsq.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by zhaoshengqi on 2019/10/24.
 */
public class KafkaProducerExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.30.60:9092");
        //props.put("group.id", "test");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>("orderTopic", Integer.toString(i), "hi this is kafka produce"+Integer.toString(i)));

        producer.close();
    }
}
