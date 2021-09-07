package com.alu4r.spendreport;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * 欺诈行为检测业务类
 *
 * @author liulu40
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    /**
     * ValueState 是一个包装类，类似于 Java 标准库里边的 AtomicReference 和 AtomicLong。
     * update 用于更新状态，value 用于获取状态值，还有 clear 用于清空状态
     * 容错处理将在 Flink 后台自动管理，你可以像与常规变量那样与状态变量进行交互
     * <p>
     * 此处用于标记是否需要发出警告
     */
    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    /**
     * 一个生命周期的方法，被调用一次，用于状态注册
     *
     * @param parameters 配置
     */
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Boolean lastTransactionWasSmall = flagState.value();
        // 检查是否之前进行过小额交易
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            // 清楚定时器
            cleanUp(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // 账户小额交易状态设置为true
            flagState.update(true);
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            // 注册一个定时器到点执行 onTimer方法
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // 定时器1分钟失效
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        Long timer = timerState.value();
        // 删除定时器
        ctx.timerService().deleteProcessingTimeTimer(timer);

        timerState.clear();
        flagState.clear();
    }
}