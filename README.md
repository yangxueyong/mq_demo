# Getting Started

一个生产端 K，三个消费端 A B C， A消费端对应两个读队列 A1 A2，B对应的读队列为B1，C对应的读队列为C1
当C消费端获取到消息后，一直处于阻塞状态时，此时C对应的C1队列不应该再接收新的消息，然而Rocketmq并不能做到当C1队列中的消息过多时不再将消息发送到C1队列的功能（除非C掉线）。
因此该项目采取的策略是：生产端每隔五秒去获取每个队列的消费状态，收集那些相对空闲的队列，下次发送消息时，则将消息发送到这些空闲队列中即可。

* 1，启动rocketmq，并创建user_msg
* 2，依次启动mq_consumer_demo1，mq_consumer_demo2，mq_consumer_demo3，mq_provider_demo
* 3，多次请求该接口 http://127.0.0.1:8080/mq/send/100


