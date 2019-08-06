package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;


/**
 * first n： 获取集合中的前N个元素
 * Sort Partition：在本地对数据集的所有分区进行排序，通过sortPartition()的链接调用来完成对多个字段的排序
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoFirstNJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> data = new ArrayList<>();

        data.add(new Tuple2<>(2,"ls"));
        data.add(new Tuple2<>(4,"aa"));
        data.add(new Tuple2<>(3,"hehe"));
        data.add(new Tuple2<>(1,"zsdee"));
        data.add(new Tuple2<>(1,"zsss"));
        data.add(new Tuple2<>(1,"zsdd"));


        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data);
        //打印
        text1.print();
        System.out.println("==============================");

        //获取前三个数据
        text1.first(3).print();
        System.out.println("==============================");

        //根据数据中的第一列进行分组，获取每组的前2个元素
        text1.groupBy(0).first(2).print();
        System.out.println("==============================");

        //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
        text1.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("==============================");

        //不分组，全局排序获取集合中的前3个元素，针对第一个元素升序，第二个元素倒序
        text1.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();


    }
}
