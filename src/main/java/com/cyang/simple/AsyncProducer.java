package com.cyang.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * 异步发送
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("group_test");
        producer.setNamesrvAddr("47.106.142.138:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message msg = new Message("topicA",
                    "TagA",
                    "keysA",
                    ("This is msg " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            final int finalI = i;
            producer.send(msg, new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%s%n", sendResult);
                }

                public void onException(Throwable e){
                    System.out.printf("%-10d Exception %s %n", finalI, e);
                    e.printStackTrace();
                }
            });
        }
        TimeUnit.SECONDS.sleep(10);
        producer.shutdown();
    }
}
