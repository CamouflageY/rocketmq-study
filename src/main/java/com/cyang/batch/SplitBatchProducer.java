package com.cyang.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 批量消息-超过4m-生产者
 */
public class SplitBatchProducer {
    public static void main(final String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducer2");
        producer.setNamesrvAddr("47.106.142.138:9876");
        producer.start();

        List<Message> list = new ArrayList<Message>(100 * 1000);
        for (int i = 0; i < 100 * 1000; i++) {
            Message message = new Message("BatchTest", "Tag", "Key" + i, ("This is msg " + i).getBytes());
            list.add(message);
        }

        // 把大的消息分成若干个小的消息 1M左右
        ListSplitter splitter = new ListSplitter(list);
        while (splitter.hasNext()){
            List<Message> messageList = splitter.next();
            producer.send(messageList);
            TimeUnit.MILLISECONDS.sleep(100);
        }

        TimeUnit.SECONDS.sleep(10);
        producer.shutdown();
    }
}
