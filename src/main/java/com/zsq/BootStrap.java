package com.zsq;

import com.zsq.controller.MsgProducer;
import org.apache.kafka.clients.producer.internals.Sender;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoshengqi on 2017/4/18.
 */
@SpringBootApplication
@Configuration
@EnableKafka
public class BootStrap {
    public static void main(String[] args) throws InterruptedException {

        ApplicationContext app = SpringApplication.run(BootStrap.class, args);

        while(true){
            MsgProducer mp = app.getBean(MsgProducer.class);
            mp.sendMessage();
            TimeUnit.SECONDS.sleep(3);
            //Thread.sleep(500);
        }
    }
}
