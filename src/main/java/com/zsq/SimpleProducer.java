package com.zsq;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by zhaoshengqi on 2017/4/18.
 */
public class SimpleProducer extends Thread{

    public static void main(String[] args) {
        SimpleProducer sp = new SimpleProducer("test2");
        sp.start();
    }

    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public SimpleProducer(String topic) {
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("metadata.broker.list", "localhost:9092");
        props.put("acks", "all");//ack方式，all，会等所有的commit最慢的方式
        props.put("retries", 0);//失败是否重试，设置会有可能产生重复数据
        props.put("batch.size", 16384);//对于每个partition的batch buffer大小
        props.put("linger.ms", 1);//等多久，如果buffer没满，比如设为1，即消息发送会多1ms的延迟，如果buffer没满
        props.put("buffer.memory", 33554432);//整个producer可以用于buffer的内存大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = new String("Message_" + messageNo);
            System.out.println("Send:" + messageStr);
            producer.send(new ProducerRecord<Integer, String>(topic, messageStr));
            messageNo++;
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
