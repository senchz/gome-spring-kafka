package com.zsq.simple;

/**
 * Created by zhaoshengqi on 2019/10/24.
 */
public class KafkaConsumerExample {

    public static void main(String[] args) {

        /*Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.30.60:9092");
        props.put("group.id", "1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orderTopic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }*/
        String str1 = new StringBuilder("i'm").append(" T").toString();
        System.out.println(str1.intern()==str1);

        String str2 = new StringBuilder("ja").append("va").toString();
        System.out.println(str2.intern()==str2);

    }
}
