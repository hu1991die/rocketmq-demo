package com.feizi.simple;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 同步生产
 * Created by feizi Ruan on 2017/9/25.
 */
public class AsyncProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("simple_async_produce_group_feizi");

        //set name server address.
        producer.setNamesrvAddr("10.0.4.64:9876");

        //Launch the instance.
        producer.start();

        producer.setRetryTimesWhenSendFailed(0);
        for (int i = 0; i < 2; i++){
            final int index = i;
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("Topic_Feizi_test",
                    "TagA",
                    "OrderID188",
                    "AsyncProducer ——> Hello world".getBytes(MixAll.DEFAULT_CHARSET));

            // 同步发送， 设置回调
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }

        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}
