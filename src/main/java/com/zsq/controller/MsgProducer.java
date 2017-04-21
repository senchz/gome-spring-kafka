package com.zsq.controller;

import com.alibaba.fastjson.JSON;
import com.zsq.bean.Message;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.UUID;

/**
 * Created by zhaoshengqi on 2017/4/18.
 */
@Component
public class MsgProducer {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    public void sendMessage(){
        Message m = new Message();
        m.setId(System.currentTimeMillis());
        m.setMsg(UUID.randomUUID().toString());
        m.setSendTime(new Date());
        kafkaTemplate.send("test2", JSON.toJSONString(m));

        kafkaTemplate.metrics();//监控


        kafkaTemplate.execute(new KafkaOperations.ProducerCallback<String, String, Object>() {
            @Override
            public Object doInKafka(Producer<String, String> producer) {
                //这里可以编写kafka原生的api操作

                return null;
            }
        });

        //消息发送的监听器，用于回调返回信息
        kafkaTemplate.setProducerListener(new ProducerListener<String, String>() {
            @Override
            public void onSuccess(String topic, Integer partition, String key, String value, RecordMetadata recordMetadata) {
                System.out.println( "SEND MESSAGE SUCCESS ! topic = [" + topic + "], partition = [" + partition + "], key = [" + key + "], value = [" + value + "], recordMetadata = [" + recordMetadata + "]");
            }

            @Override
            public void onError(String topic, Integer partition, String key, String value, Exception exception) {
                System.out.println("SEND MESSAGE ERROR ! topic = [" + topic + "], partition = [" + partition + "], key = [" + key + "], value = [" + value + "], exception = [" + exception + "]");
            }

            /**
             * 方法返回值代表是否启动kafkaProducer监听器
             */
            @Override
            public boolean isInterestedInSuccess() {
                return true;
            }
        });
    }
}
