package com.zsq.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoshengqi on 2019/10/24.
 */
public class ProducerThread extends Thread{

    private String servers;

    public ProducerThread(String server){
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
        //props.put("group.id", "test");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        int i=0;
        while (true){
            i++;
            String value = "hi this is kafka produce"+Integer.toString(i);
            try {
                long starttime = System.currentTimeMillis();
                Future<RecordMetadata> rm = producer.send(new ProducerRecord<String, String>("test2017", Integer.toString(i), value));
                System.out.println("~~~~~~~~~~~~time="+(System.currentTimeMillis()-starttime));
                System.out.println(rm.get().toString());
                System.out.println("Product success, value = "+value );
                TimeUnit.MILLISECONDS.sleep(3000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }
}
