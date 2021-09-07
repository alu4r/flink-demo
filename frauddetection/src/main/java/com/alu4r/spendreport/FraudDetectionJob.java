package com.alu4r.spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * 在当今数字时代，信用卡欺诈行为越来越被重视。 罪犯可以通过诈骗或者入侵安全级别较低系统来盗窃信用卡卡号。
 * 用盗得的信用卡进行很小额度的例如一美元或者更小额度的消费进行测试。
 * 如果测试消费成功，那么他们就会用这个信用卡进行大笔消费，来购买一些他们希望得到的，或者可以倒卖的财物。
 * 在这个教程中，你将会建立一个针对可疑信用卡交易行为的反欺诈检测系统。 通过使用一组简单的规则，
 * 你将了解到 Flink 如何为我们实现复杂业务逻辑并实时执行。
 *
 * @author alu4r
 **/
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        // 1.执行环境
        // 用于设置执行环境。任务执行环境用于定义任务的属性、创建数据源以及最终启动任务的执行。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建数据源
        // 数据源从外部系统例如 Apache Kafka、Rabbit MQ 或者 Apache Pulsar 接收数据，然后将数据送到 Flink 程序中。
        // 这个代码练习使用的是一个能够无限循环生成信用卡模拟交易数据的数据源。
        // 每条交易数据包括了信用卡 ID （accountId），交易发生的时间 （timestamp） 以及交易的金额（amount）。
        // 绑定到数据源上的 name 属性是为了调试方便，如果发生一些异常，我们能够通过它快速定位问题发生在哪里。
        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        // 3.对事件分区 & 欺诈检测
        // transactions 这个数据流包含了大量的用户交易数据，需要被划分到多个并发上进行欺诈检测处理。
        // 由于欺诈行为的发生是基于某一个账户的，所以，必须要要保证同一个账户的所有交易行为数据要被同一个并发的 task 进行处理。
        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        // 4.输出结果
        // sink 会将 DataStream 写出到外部系统，例如 Apache Kafka、Cassandra 或者 AWS Kinesis 等。
        // AlertSink 使用 INFO 的日志级别打印每一个 Alert 的数据记录，而不是将其写入持久存储，以便你可以方便地查看结果。
        alerts.addSink(new AlertSink())
                .name("send-alerts");

        // 5.运行作业
        // Flink 程序是懒加载的，并且只有在完全搭建好之后，才能够发布到集群上执行。
        // 调用 StreamExecutionEnvironment#execute 时给任务传递一个任务名参数，就可以开始运行任务。
        env.execute("Fraud Detection");
    }
}
