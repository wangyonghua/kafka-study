package com.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * Created by wangyonghua on 2017/11/17.
 */
public class KafkaProduce extends Thread {
    private String topic;

    public KafkaProduce(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer producer = createProducer();
        for (int i = 0; i < 100; i++) {
            producer.send(new KeyedMessage(topic, "hello world"));
        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "121.42.199.81:2181");
        properties.setProperty("serializer.class", StringEncoder.class.getName());
        properties.setProperty("metadata.broker.list", "121.42.199.81:9092");
        return new Producer(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new KafkaProduce("example5").start();
    }
}
