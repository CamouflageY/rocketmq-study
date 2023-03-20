package com.cyang.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 批量消息-生产者
 */
public class BatchProducer {
    public static void main(final String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducer");
        producer.setNamesrvAddr("47.106.142.138:9876");
        producer.start();

        List<Message> list = new ArrayList<Message>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("BatchTest", "Tag", "Key" + i, ("This is msg " + i).getBytes());
            list.add(message);
        }
        try {
            producer.send(list);
        }catch (Exception e){
            producer.shutdown();
            e.printStackTrace();
        }
        TimeUnit.SECONDS.sleep(10);
        producer.shutdown();
    }
}
