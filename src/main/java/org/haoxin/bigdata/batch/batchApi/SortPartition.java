package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/3/20 14:52
 */
public class SortPartition {
    public static void main(String[] args) throws  Exception{
        //创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //构建数据source
        ArrayList<Tuple2<Integer,String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"zs"));
        data.add(new Tuple2<>(4,"ls"));
        data.add(new Tuple2<>(3,"ww"));
        data.add(new Tuple2<>(1,"xw"));
        data.add(new Tuple2<>(1,"aw"));
        data.add(new Tuple2<>(1,"mw"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        //获取前三条数据
        text.first(3).print();
        System.out.println("====================================");

        //根据数据中第一列分组，获取每组的前2个元素
        text.groupBy(0).first(2).print();
        System.out.println("====================================");


        //根据数据中第一列分组，z再根据第二列进行组内排序（升序），获取每组的前2个元素
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("====================================");

        //不分组，全局排序获取前3个元素，针对第一个元素升序，第二个元素倒序
        text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();
        System.out.println("====================================");

    }
}
