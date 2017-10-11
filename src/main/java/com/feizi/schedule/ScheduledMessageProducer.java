package com.feizi.schedule;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * 定时消息发送
 * Created by feizi Ruan on 2017/10/9.
 */
public class ScheduledMessageProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // Instantiate a producer to send schedule messages
        DefaultMQProducer producer = new DefaultMQProducer("ScheduledMessageProducer_feizi");

        // set the nameserver address
        producer.setNamesrvAddr("10.0.4.64:9876");

        // launch producer
        producer.start();

        int totalMessagesToSend = 3;
        for (int i = 0; i < totalMessagesToSend; i++){
            Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());
            // This message will be delivered to consumer 10 seconds later.
            message.setDelayTimeLevel(4);

            //send the message
            producer.send(message);
        }
        // Shutdown producer after use.
        producer.shutdown();
    }
}
