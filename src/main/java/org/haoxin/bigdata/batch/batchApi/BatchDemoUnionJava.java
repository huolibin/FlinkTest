package org.haoxin.bigdata.batch.batchApi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;


/**
 * union 数据源的类型一样的相连接
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoUnionJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"aa"));
        data1.add(new Tuple2<>(4,"hehe"));

        //id city
        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(5,"guangzhou"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);

        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        UnionOperator<Tuple2<Integer, String>> unionData = text1.union(text2);

        unionData.print();


    }
}
