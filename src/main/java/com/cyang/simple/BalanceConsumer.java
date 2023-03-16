package com.cyang.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 集群消费
 */
public class BalanceConsumer {

    public static void main(String[] args) throws Exception{
        // 创建消费者consumer，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_consumer");
        // 指定nameserver地址
        consumer.setNamesrvAddr("47.106.142.138:9876");
        //一个订阅服务器A
//        consumer.setUnitName("consumer1");
        //一个订阅服务器B
//        consumer.setUnitName("consumer2");

        // 订阅主题Topic和Tag
        consumer.subscribe("TopicTest", "*");
        // 负载均衡模式消费
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    for(MessageExt msg : list){
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tags = msg.getTags();
                        Thread.sleep(1000);
                        System.out.println("收到信息：" + "topic：" + topic + ", tags:" + tags + ", msg:" + msgBody);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
               return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者consumer
        consumer.start();
        System.out.printf("Consumer started.%n");
    }


}
