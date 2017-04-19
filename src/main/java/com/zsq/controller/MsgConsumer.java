package com.zsq.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zsq.bean.Message;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Created by zhaoshengqi on 2017/4/18.
 */
@Component
public class MsgConsumer {

    @KafkaListener(topics = "test2")
    public void processMessage(String content) {
        Message m = JSONObject.parseObject(content,Message.class);
        System.out.println("CONSUME SUCCESS! content = [" + content + "]");
    }
}
