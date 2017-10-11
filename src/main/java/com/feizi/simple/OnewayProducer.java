package com.feizi.simple;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * Created by feizi Ruan on 2017/9/25.
 */
public class OnewayProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("simple_produce_group_feizi");

        //set name server address.
        producer.setNamesrvAddr("10.0.4.64:9876");

        //Launch the instance.
        producer.start();

        for (int i = 0; i < 2; i++){
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("Topic_Feizi_test" /* Topic */,
                    "TagA" /* Tag */,
                    ("OnewayProducer -> Hello RocketMQ " +
                            i).getBytes(MixAll.DEFAULT_CHARSET) /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            producer.sendOneway(msg);
        }

        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}
