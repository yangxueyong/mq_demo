package com.yxy.demo.mq;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yxy.demo.config.annotation.MultiMQAdminCmdMethod;
import com.yxy.demo.entity.mq.DefQueueStatInfo;
import com.yxy.demo.entity.mq.QueueStatInfo;
import com.yxy.demo.entity.mq.TopicConsumerInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.propagate;

@Slf4j
@Component
public class MQCalcQueue {
    @Resource
    protected MQAdminExt mqAdminExt;

    private final static String CONSUMER_INFO = "CONSUMER_INFO";
    private final static String QUEUE_INFO = "QUEUE_INFO";
    public static DefQueueStatInfo defQueueStatInfo = null;
    public static List<Integer> QUEUE_THRESHOLD_LIST = new ArrayList<>();
    static{
        QUEUE_THRESHOLD_LIST.add(0);
        QUEUE_THRESHOLD_LIST.add(5);
        QUEUE_THRESHOLD_LIST.add(10);
        QUEUE_THRESHOLD_LIST.add(50);
        QUEUE_THRESHOLD_LIST.add(100);
        QUEUE_THRESHOLD_LIST.add(200);
        QUEUE_THRESHOLD_LIST.add(300);
        QUEUE_THRESHOLD_LIST.add(500);
        QUEUE_THRESHOLD_LIST.add(1000);
    }

    public List<TopicConsumerInfo> queryConsumeStatsListByGroupName(String groupName) {
        return queryConsumeStatsList(null, groupName);
    }

    @MultiMQAdminCmdMethod
    public List<TopicConsumerInfo> queryConsumeStatsList(final String topic, String groupName) {
        return (List<TopicConsumerInfo>) queryConsumeData(topic,groupName).get(CONSUMER_INFO);
    }

    @MultiMQAdminCmdMethod
    public List<QueueStatInfo> queryQueueList(final String topic, String groupName) {
        return (List<QueueStatInfo>) queryConsumeData(topic,groupName).get(QUEUE_INFO);
    }

    /**
     * 计算的主方法
     * @param topic
     * @param groupName
     */
    public void calc(final String topic, String groupName) {
        try {
            queryAndSetDefQueueList(topic, groupName);
        }catch (Exception e){
            e.printStackTrace();
            clean();
        }
    }

    /**
     * 清空数据
     */
    public void clean(){
        synchronized (defQueueStatInfo) {
            defQueueStatInfo = null;
        }
    }

    public void queryAndSetDefQueueList(final String topic, String groupName) {
        //所有的队列
        List<QueueStatInfo> list = (List<QueueStatInfo>)queryConsumeData(topic, groupName).get(QUEUE_INFO);
        if(list == null || list.size() == 0){
            clean();
            return;
        }
        System.out.println("list->json->" + JSON.toJSONString(list));
        //客户端所对应的所有队列
        Map<String, List<QueueStatInfo>> queueStatMap = list.stream().collect(Collectors.groupingBy(QueueStatInfo::getClientInfo));
        //获取每个客户端的剩余消费量（每个消费端可能有多个消费队列）
        Map<String, LongSummaryStatistics> queueStatOffsetMap =
                list.stream().collect(
                        Collectors.groupingBy(QueueStatInfo:: getClientInfo, Collectors.summarizingLong(QueueStatInfo :: getOffset)));

        DefQueueStatInfo queueStatInfo = null;

        queueStatInfo = getQueueStatInfosByConsumer(queueStatMap, queueStatOffsetMap);
        if(queueStatInfo != null){
            System.out.println("当前根据消费端选中的通道为1->" + JSON.toJSONString(queueStatInfo));
            defQueueStatInfo = queueStatInfo;
            return;
        }

        queueStatInfo = getQueueStatInfosByQueue(queueStatOffsetMap,list);
        if(queueStatInfo != null){
            System.out.println("当前根据消费端选中的通道为2->" + JSON.toJSONString(queueStatInfo));
            defQueueStatInfo = queueStatInfo;
            return;
        }

        queueStatInfo = getQueueStatInfosByAllQueue(list);
        if(queueStatInfo != null){
            System.out.println("当前根据消费端选中的通道为3->" + JSON.toJSONString(queueStatInfo));
            defQueueStatInfo = queueStatInfo;
            return;
        }

        clean();
    }

    private DefQueueStatInfo getQueueStatInfosByAllQueue(List<QueueStatInfo> list) {
        final List<QueueStatInfo> collect1 = new ArrayList<>();
        final List<QueueStatInfo> collect2 = new ArrayList<>();
        //获取平均值
        double avg = list.stream().mapToLong(QueueStatInfo::getOffset).average().getAsDouble();
        list.forEach(k->{
            if(k.getOffset() < avg){
                collect2.add(k);
            }
            if(k.getOffset() <= avg){
                collect1.add(k);
            }
        });
        return getDefQueueStatInfoByTwoList(collect2, collect1);
    }

    /**
     * 根据消费队列获取能够使用的队列
     * @param queueStatOffsetMap
     * @param datas
     * @return
     */
    private DefQueueStatInfo getQueueStatInfosByQueue(Map<String, LongSummaryStatistics> queueStatOffsetMap,List<QueueStatInfo> datas) {
        //获取所有空闲的队列
        AtomicBoolean flag = new AtomicBoolean(false);
        AtomicLong bottomSum = new AtomicLong();
        datas.forEach(t->{
            flag.set(false);
            queueStatOffsetMap.forEach((k,v)->{
                if(v.getSum() <= t.getOffset()){
                    bottomSum.set(v.getSum());
                    flag.set(true);
                    return;
                }
            });
            if(flag.get()){
                return;
            }
        });

        final List<QueueStatInfo> collect1 = new ArrayList<>();
        final List<QueueStatInfo> collect2 = new ArrayList<>();
        //获取所有比最低值还小的值
        datas.forEach(k ->{
            if(k.getOffset() < bottomSum.get()){
                collect2.add(k);
            }
            if(k.getOffset() <= bottomSum.get()){
                collect1.add(k);
            }
        });

        return getDefQueueStatInfoByTwoList(collect2, collect1);
    }

    /**
     * 根据消费端获取能够使用的队列
     * @param queueStatMap
     * @param queueStatOffsetMap
     * @return
     */
    private DefQueueStatInfo getQueueStatInfosByConsumer(Map<String, List<QueueStatInfo>> queueStatMap, Map<String, LongSummaryStatistics> queueStatOffsetMap) {
        //获取所有空闲的队列
        AtomicBoolean flag = new AtomicBoolean(false);
        AtomicLong bottomSum = new AtomicLong();
        QUEUE_THRESHOLD_LIST.forEach(t->{
            flag.set(false);
            queueStatOffsetMap.forEach((k,v)->{
                if(v.getSum() <= t){
                    bottomSum.set(v.getSum());
                    flag.set(true);
                    return;
                }
            });
            if(flag.get()){
                return;
            }
        });

        final List<QueueStatInfo> collect2 = new ArrayList<>();
        final List<QueueStatInfo> collect1 = new ArrayList<>();

        //获取所有比最低值还小的值
        queueStatOffsetMap.forEach((k,v) ->{
            if(v.getSum() < bottomSum.get()){
                collect2.addAll(queueStatMap.get(k));
            }
            if(v.getSum() <= bottomSum.get()){
                collect1.addAll(queueStatMap.get(k));
            }
        });

        return getDefQueueStatInfoByTwoList(collect2, collect1);
    }

    private DefQueueStatInfo getDefQueueStatInfoByTwoList(List<QueueStatInfo> collect2, List<QueueStatInfo> collect1) {
        DefQueueStatInfo defQueueStatInfo = new DefQueueStatInfo();
        if (collect2 != null && collect2.size() > 0) {
            collect2.sort(Comparator.comparingLong(QueueStatInfo::getOffset));
            QueueStatInfo q = collect2.get(new Random().nextInt(collect2.size()));
            defQueueStatInfo.setOptionalQueueStatInfos(collect2);
            defQueueStatInfo.setQueueStatInfo(q);
            return defQueueStatInfo;
        }

        if (collect1 != null && collect1.size() > 0) {
            collect1.sort(Comparator.comparingLong(QueueStatInfo::getOffset));
            QueueStatInfo q = collect1.get(new Random().nextInt(collect1.size()));
            defQueueStatInfo.setOptionalQueueStatInfos(collect1);
            defQueueStatInfo.setQueueStatInfo(q);
            return defQueueStatInfo;
        }
        return null;
    }

    public QueueStatInfo getDefQueueStatInfoByList() {
        if (defQueueStatInfo == null){
            return null;
        }
        List<QueueStatInfo> optionalQueueStatInfos = defQueueStatInfo.getOptionalQueueStatInfos();
        if (optionalQueueStatInfos != null && optionalQueueStatInfos.size() > 0) {
            QueueStatInfo q = optionalQueueStatInfos.get(new Random().nextInt(optionalQueueStatInfos.size()));
            return q;
        }
        return null;
    }

    public Map queryConsumeData(final String topic, String groupName){
        ConsumeStats consumeStats = null;
        try {
            consumeStats = mqAdminExt.examineConsumeStats(groupName, topic);
        }catch (Exception e) {
            throw propagate(e);
        }
        List<MessageQueue> mqList = Lists.newArrayList(Iterables.filter(consumeStats.getOffsetTable().keySet(), o -> StringUtils.isBlank(topic) || o.getTopic().equals(topic)));
        Collections.sort(mqList);

        Map<String,Object> resultMap = new HashMap<>();
        List<TopicConsumerInfo> topicConsumerInfoList = Lists.newArrayList();
        List<QueueStatInfo> queueStatInfos = Lists.newArrayList();

        resultMap.put(CONSUMER_INFO,topicConsumerInfoList);
        resultMap.put(QUEUE_INFO,queueStatInfos);

        TopicConsumerInfo nowTopicConsumerInfo = null;
        Map<MessageQueue, String> messageQueueClientMap = getClientConnection(groupName);


        for (MessageQueue mq : mqList) {
            if (nowTopicConsumerInfo == null || (!StringUtils.equals(mq.getTopic(), nowTopicConsumerInfo.getTopic()))) {
                nowTopicConsumerInfo = new TopicConsumerInfo(mq.getTopic());
                topicConsumerInfoList.add(nowTopicConsumerInfo);
            }
            QueueStatInfo queueStatInfo = QueueStatInfo.fromOffsetTableEntry(mq, consumeStats.getOffsetTable().get(mq));
            if(StringUtils.isNotEmpty(topic) && topic.equals(mq.getTopic())) {
                queueStatInfo.setMq(mq);
                queueStatInfo.setOffset(queueStatInfo.getBrokerOffset() - queueStatInfo.getConsumerOffset());
                queueStatInfos.add(queueStatInfo);
            }
            queueStatInfo.setClientInfo(messageQueueClientMap.get(mq));
            nowTopicConsumerInfo.appendQueueStatInfo(queueStatInfo);
        }
        if(queueStatInfos != null) {
            queueStatInfos.sort(Comparator.comparingLong(QueueStatInfo::getOffset));
        }
        return resultMap;
    }

    private Map<MessageQueue, String> getClientConnection(String groupName) {
        Map<MessageQueue, String> results = Maps.newHashMap();
        try {
            ConsumerConnection consumerConnection = mqAdminExt.examineConsumerConnectionInfo(groupName);
            for (Connection connection : consumerConnection.getConnectionSet()) {
                String clinetId = connection.getClientId();
                ConsumerRunningInfo consumerRunningInfo = mqAdminExt.getConsumerRunningInfo(groupName, clinetId, false);
                for (MessageQueue messageQueue : consumerRunningInfo.getMqTable().keySet()) {
//                    results.put(messageQueue, clinetId + " " + connection.getClientAddr());
                    results.put(messageQueue, clinetId);
                }
            }
        }
        catch (Exception err) {
            log.error("op=getClientConnection_error", err);
        }
        return results;
    }
}
