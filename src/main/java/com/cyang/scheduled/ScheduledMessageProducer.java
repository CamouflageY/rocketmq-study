package com.cyang.scheduled;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 延时消息-生产者
 */
public class ScheduledMessageProducer {
    public static void main(final String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ScheduledProducer");
        producer.setNamesrvAddr("47.106.142.138:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message msg = new Message("ScheduledTopic", ("This is msg" + i).getBytes());
            // 设置延迟等级 即30s之后发送给消费者
            // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            msg.setDelayTimeLevel(4);
            producer.send(msg);
        }
        TimeUnit.SECONDS.sleep(10);
        producer.shutdown();
    }
}
