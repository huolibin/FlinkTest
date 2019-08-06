package org.haoxin.bigdata.streaming.customSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义实现并行度source
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/16 18:17
 */
public class MyParallelSourceFunction implements ParallelSourceFunction<Long> {
    public Long count =1L;
    private boolean isRunning = true;
    /**
     *
     * @param sourceContext
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都一样字啊这个run中实现一个循环，这样就可以循环产生数据了
     *
     */
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个cancel的时候会调用的方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
