package com.feizi.batch;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 批量消息消费
 * Created by feizi Ruan on 2017/10/9.
 */
public class BatchMessageConsumer {
    public static void main(String[] args) throws MQClientException {
        //Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("BatchMessageConsumer_feizi");

        //Specify where to start in case the specified consumer group is a brand new one.
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //Subscribe one more more topics to consume
        consumer.subscribe("BatchTopicTest", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + "->Receive New Message: " + msgs + "%n");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // set the nameserver address.
        consumer.setNamesrvAddr("10.0.4.64:9876");

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("===Consumer Started.%n");
    }
}
