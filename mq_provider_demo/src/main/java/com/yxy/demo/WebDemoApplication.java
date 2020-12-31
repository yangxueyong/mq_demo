package com.yxy.demo;

import com.yxy.demo.mq.MQCalcQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;

@SpringBootApplication
@EnableScheduling
public class WebDemoApplication {


    @Autowired
    private MQCalcQueue mqCalcQueue;

    private boolean flag = false;

    public static void main(String[] args) {
        SpringApplication.run(WebDemoApplication.class, args);
    }

    //3.添加定时任务
    @Scheduled(cron = "0/5 * * * * ?")
    //或直接指定时间间隔，例如：5秒
    private void configureTasks() {
        if(flag){
            return;
        }
        flag = true;
        try {
            mqCalcQueue.calc("user_msg", "yxy_unique_group_name");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            flag = false;
        }
    }
}
