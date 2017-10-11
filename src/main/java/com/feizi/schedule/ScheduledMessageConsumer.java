package com.feizi.schedule;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 定时消息
 * Created by feizi Ruan on 2017/10/9.
 */
public class ScheduledMessageConsumer {

    public static void main(String[] args) throws MQClientException {
        //Instaniate message consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ScheduledMessageConsumer_feizi");

        //Subscribe topics
        consumer.subscribe("TestTopic", "*");

        // Register message listener
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt messageExt : msgs){
                    // Print approximate delay time period
                    System.out.println("Receive message[msgId=" + messageExt.getMsgId() + "] "
                            + (System.currentTimeMillis() - messageExt.getStoreTimestamp()) + "ms later");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // set the nameserveraddress
        consumer.setNamesrvAddr("10.0.4.64:9876");

        // launch the consumer
        consumer.start();

        System.out.printf("===Consumer Started.%n");
    }
}
