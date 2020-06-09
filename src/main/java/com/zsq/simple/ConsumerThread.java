package com.zsq.simple;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by zhaoshengqi on 2019/10/24.
 */
public class ConsumerThread extends Thread{
    private String servers;

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    int count = 0;

    public ConsumerThread(String server){
        this.servers=server;
    }
    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }
    @Override
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", "0");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        /** 不使用消费者群组,直接订阅主题,自己指定分区 */
        /*List<PartitionInfo > partitionInfos = consumer.partitionsFor("topic");
        List<TopicPartition> partitions = new ArrayList<>();
        if (partitionInfos != null) {
            for (PartitionInfo partition : partitionInfos)
                partitions.add(new TopicPartition(partition.topic(),
                        partition.partition()));
            consumer.assign(partitions);
        }*/

        consumer.subscribe(Arrays.asList("test2017"), new ConsumerRebalanceListener() {
            @Override
            //消费者再均衡前和消费者停止读取消息之后被调用
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Lost partitions in rebalance. Committing current offsets:" + currentOffsets);
                consumer.commitSync(currentOffsets);
            }

            //重新分配分区后和读取消息之前被调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("starting exit```");
                consumer.wakeup();
                try {
                    this.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    System.out.printf("Consumer success. offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    //提交特性偏移量
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, "no metadata"));
                    if (count % 1000 == 0)
                        consumer.commitAsync(currentOffsets,null);
                        //consumer.commitSync(currentOffsets);
                }
                //consumer.commitAsync();//异步提交偏移量
            }

        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();//关闭后会触发消费者rebalance

            }
        }
    }
}
