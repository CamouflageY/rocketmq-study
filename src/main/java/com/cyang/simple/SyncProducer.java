package com.cyang.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 同步发送
 *
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        // 实例化消费生产者producer 并指定组名
        DefaultMQProducer producer = new DefaultMQProducer("group_test");
        // 指定nameserver地址
        producer.setNamesrvAddr("47.106.142.138:9876");
        // 启动producer
        producer.start();
        // 创建消息对象 指定Topic tag和消息体
        for (int i = 0; i < 10; i++) {
            Message msg = new Message("TopicTest",
                    "TagA",
                    ("This is msg " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 发送消息
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        Thread.sleep(10000);
        producer.shutdown();
        System.out.println("producer was shutdown");
        // 关闭生产者
    }
}
