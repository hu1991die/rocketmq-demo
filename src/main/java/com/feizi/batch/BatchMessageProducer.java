package com.feizi.batch;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量消息生产
 * Created by feizi Ruan on 2017/10/9.
 */
public class BatchMessageProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //Instantiate whih a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("BatchMessageProducer_feizi");

        //set name server address.
        producer.setNamesrvAddr("10.0.4.64:9876");

        //Launch the instance.
        producer.start();

        String topic = "BatchTopicTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderID001", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "TagB", "OrderID002", "Hello world 2".getBytes()));
        messages.add(new Message(topic, "TagC", "OrderID003", "Hello world 3".getBytes()));

        for (Message message : messages){
            producer.send(message);
        }

        //Shut down once the producer instance is not logger in use.
        producer.shutdown();
    }
}
