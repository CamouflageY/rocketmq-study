package com.cyang.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * sql过滤 -消息生产者（加入消息属性)
 */
public class SqlFilterProducer {
    public static void main(final String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("SqlFilterProducer");
        producer.setNamesrvAddr("47.106.142.138:9876");
        producer.start();

        String[] tags = new String[]{"TagA", "TagB", "TagC"};
        for (int i = 0; i < 3; i++) {
            Message msg = new Message("SqlFilterTest", tags[i % tags.length], "This is msg".getBytes(RemotingHelper.DEFAULT_CHARSET));

            // 设置sql过滤的属性
            msg.putUserProperty("a", String.valueOf(i));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        TimeUnit.SECONDS.sleep(10);
        producer.shutdown();
    }
}
