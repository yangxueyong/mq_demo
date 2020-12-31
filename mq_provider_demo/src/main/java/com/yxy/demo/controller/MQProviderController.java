package com.yxy.demo.controller;

import com.alibaba.fastjson.JSON;
import com.yxy.demo.entity.mq.DefQueueStatInfo;
import com.yxy.demo.entity.mq.QueueStatInfo;
import com.yxy.demo.mq.MQCalcQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequestMapping("/mq")
@RestController
public class MQProviderController {

    @Autowired
    private MQCalcQueue mqCalcQueue;

    @RequestMapping("/send/{N}")
    public String select(@PathVariable("N") int N) throws InterruptedException, MQClientException {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("yxy_unique_group_name");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = N;
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;

                Message msg = new Message("user_msg",
                        "TagA",
                        "OrderID188",
                        ("Hello world" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

                QueueStatInfo defQueueStatInfo = mqCalcQueue.getDefQueueStatInfoByList();
                synchronized (MQCalcQueue.defQueueStatInfo) {
                    if (defQueueStatInfo == null) {
                        producer.send(msg, 5000);
                    } else {
                        producer.send(msg, defQueueStatInfo.getMq(), 5000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
        return "ok";
    }

    @RequestMapping("/query")
    public String select() {
        String string = JSON.toJSONString(mqCalcQueue.queryConsumeStatsListByGroupName("please_rename_unique_group_name"));
        System.out.println("数据->" + JSON.toJSONString(string));
        return string;
    }


}
