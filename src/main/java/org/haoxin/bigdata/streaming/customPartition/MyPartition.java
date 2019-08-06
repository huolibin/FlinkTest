package org.haoxin.bigdata.streaming.customPartition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/18 15:56
 *  自定义分区
 *  根据数字的奇偶性来分区
 *  通过实现Partitioner<T>来完成
 */
public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数："+numPartitions);
        if (key %2 == 0){
            return 0;
        }else {
            return 1;
        }
    }
}
