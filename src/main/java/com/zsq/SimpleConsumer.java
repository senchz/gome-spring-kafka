package com.zsq;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by zhaoshengqi on 2017/4/18.
 */
public class SimpleConsumer extends Thread{
    private static volatile boolean isStop = false;

    public static void main(String[] args) {
        SimpleConsumer sc = new SimpleConsumer();
        sc.start();
    }
    @Override
    public void run(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("zookeeper.connect", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        props.put("group.id", "group1");
        props.put("enable.auto.commit", "true");  //自动commit
        props.put("auto.commit.interval.ms", "1000"); //定时commit的周期
        props.put("session.timeout.ms", "30000"); //consumer活性超时时间
        //props.put("max.poll.records", "100"); //限制一次poll的条数
        //props.put("auto.offset.reset", offset);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer.encoding", "UTF8");
        props.put("value.deserializer.encoding", "UTF8");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test2"));
        //手动commit
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        while (!isStop) {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {

                //buffer.add(record);
                try {
                    System.out.println(record);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //手动commit
            /*if (buffer.size() >= minBatchSize) {
                insertIntoDb(buffer);
                consumer.commitSync(); //批量完成写入后，手工sync offset
                buffer.clear();
            }*/
        }
        //更细粒度的commitSync
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) { //按partition处理
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition); //取出partition对应的Records
                    for (ConsumerRecord<String, String> record : partitionRecords) { //处理每条record
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset(); //取出last offset
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1))); //独立的sync每个partition的offset
                }
            }
        } finally {
            consumer.close();
        }
    }
}
