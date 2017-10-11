package com.feizi.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;

import java.io.UnsupportedEncodingException;

/**
 * 消息生产者
 * Created by feizi Ruan on 2017/7/8.
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, InterruptedException {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("consumer_group_feizi");

        //set name server address.
        producer.setNamesrvAddr("10.0.4.64:9876");

        //set instance name.
        producer.setInstanceName("Producer-Feizi");

        //Launch the instance.
        producer.start();

        for (int i = 0; i < 2; i++){
            try {
                //Create a message instance, specifying topic, tag and message body.
                Message msg = new Message("TOPIC_FEIZI_TEST",
                        "TagA",
                        ("====Hello RocketMQ" + i).getBytes(MixAll.DEFAULT_CHARSET));
                msg.setKeys("feizi");

                //Call send message to deliver message to one of brokers.
                SendResult sendResult = producer.send(msg);
                System.out.printf("===>发送结果：result:%s%n", sendResult);
            }catch (Exception e){
                e.printStackTrace();
                Thread.sleep(1000);
            }

        }

        //Shut down once the producer instance is not logger in use.
        producer.shutdown();
    }
}
