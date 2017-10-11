package com.feizi.quickstart;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消息消费者
 * Created by feizi Ruan on 2017/7/8.
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        //Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group_feizi_12345");

        //Specify where to start in case the specified consumer group is a brand new one.
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //Subscribe one more more topics to consume
        consumer.subscribe("TOPIC_FEIZI_TEST", "TagA");

        //Register callback to execute on arrival of message fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + "->Receive New Message: " + msgs + "%n");
                if(null != msgs && msgs.size() > 0){
                    System.out.println("\n");
                    for (MessageExt messageExt : msgs){
                        System.out.println("getBody: " + new String(messageExt.getBody()));
                        System.out.println("getBornHost: " + messageExt.getBornHost());
                        System.out.println("getMsgId: " + messageExt.getMsgId());
                        System.out.println("getQueueId: " + messageExt.getQueueId());
                        System.out.println("getQueueOffset: " + messageExt.getQueueOffset());
                        System.out.println("getStoreHost: " + messageExt.getStoreHost());
                        System.out.println("getTags: " + messageExt.getTags());
                        System.out.println("getTopic: " + messageExt.getTopic());
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //set name server address.
        consumer.setNamesrvAddr("10.0.4.64:9876");

        //set instance name.
        consumer.setInstanceName("Consumer-Feizi");

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("===Consumer Started.%n");
    }
}
