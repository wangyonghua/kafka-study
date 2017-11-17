package com.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangyonghua on 2017/11/16.
 */
public class KafkaConsumer extends Thread {
    private String topic;

    public KafkaConsumer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
        while(iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println("接收到: " + message);
        }


    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "121.42.199.81:2181");
        properties.setProperty("group.id", "group1");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) {
        new KafkaConsumer("example5").start();
    }
}
