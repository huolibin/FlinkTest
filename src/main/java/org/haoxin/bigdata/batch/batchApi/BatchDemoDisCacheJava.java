package org.haoxin.bigdata.batch.batchApi;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 *
 * distributed Cache 分布式缓存
 * 此缓存的工作机制如下：程序注册一个文件或者目录(本地或者远程文件系统，例如hdfs或者s3)，通过ExecutionEnvironment注册缓存文件并为它起一个名称。当程序执行，Flink自动将文件或者目录复制到所有taskmanager节点的本地文件系统，用户可以通过这个指定的名称查找文件或者目录，然后从taskmanager节点的本地文件系统访问它
 * 用法
 * 1：注册一个文件
 * env.registerCachedFile("hdfs:///path/to/your/file", "hdfsFile")
 * 2：访问数据
 * File myFile = getRuntimeContext().getDistributedCache().getFile("hdfsFile");
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/31 16:23
 */
public class BatchDemoDisCacheJava {
    public static void main(String[] args)  throws Exception{

        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1.注册一个文件
        env.registerCachedFile("e:\\ceshi\\files\\a.txt","afile" );

        //源数据
        DataSource<String> data = env.fromElements("a", "b", "c");

        //对数据做笛卡尔积操作
        MapOperator<String, Tuple2<String, String>> mapData = data.map(new RichMapFunction<String, Tuple2<String, String>>() {
            ArrayList<String> list = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2.访问数据
                File myfile = getRuntimeContext().getDistributedCache().getFile("afile");
                //对file数据处理
                List<String> strings = FileUtils.readLines(myfile);
                for (String s : strings) {
                    System.out.println("lines:" + s);
                    list.add(s);
                }
            }

            @Override
            public Tuple2<String, String> map(String value) {
                for (String s : list) {
                    return new Tuple2<>(value, s);
                }
                return null;
            }
        });

        mapData.print();
    }
}
