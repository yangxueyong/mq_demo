package com.yxy.demo.entity.mq;


import java.util.List;


public class DefQueueStatInfo {
    private QueueStatInfo queueStatInfo;
    private List<QueueStatInfo> optionalQueueStatInfos;

    public QueueStatInfo getQueueStatInfo() {
        return queueStatInfo;
    }

    public void setQueueStatInfo(QueueStatInfo queueStatInfo) {
        this.queueStatInfo = queueStatInfo;
    }

    public List<QueueStatInfo> getOptionalQueueStatInfos() {
        return optionalQueueStatInfos;
    }

    public void setOptionalQueueStatInfos(List<QueueStatInfo> optionalQueueStatInfos) {
        this.optionalQueueStatInfos = optionalQueueStatInfos;
    }
}
