package com.feizi.simple;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 异步生产
 * Created by feizi Ruan on 2017/9/25.
 */
public class SyncProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        // Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("simple_sync_produce_group_feizi");

        //set name server address.
        producer.setNamesrvAddr("10.0.4.64:9876");

        //Launch the instance.
        producer.start();

        for (int i = 0; i < 2; i++){
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("Topic_Feizi_test",/* Topic */
                    "TagA",/* Tag */
                    ("SyncProducer -> Hello RocketMq" + i).getBytes(MixAll.DEFAULT_CHARSET));/* Message body */

            // call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}
