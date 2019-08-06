package org.haoxin.bigdata.streaming.customSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义并行度source
 * 继承RichParallelSourceFunction
 * RichParallelSourceFunction会额外提供open和close方法
 * 针对source中如果需要获取其他链接资源，那么可以在open方法中获取资源链接，在close中关闭资源链接
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/16 18:18
 */
public class MyRichParallelSourceFunction extends RichParallelSourceFunction<Long> {

    public Long count =1L;
    private boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
