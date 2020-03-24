package org.haoxin.bigdata;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink提供了一个分布式缓存，类似于hadoop，可以使用户在并行函数中很方便的读取本地文件，并把它放在taskmanager节点中，防止task重复拉取。
 * 此缓存的工作机制如下：程序注册一个文件或者目录(本地或者远程文件系统，例如hdfs或者s3)，通过ExecutionEnvironment注册缓存文件并为它起一个名称。
 * 当程序执行，Flink自动将文件或者目录复制到所有taskmanager节点的本地文件系统，仅会执行一次。
 * 用户可以通过这个指定的名称查找文件或者目录，然后从taskmanager节点的本地文件系统访问它。
 */

/**
 * flink分布式缓存的应用,
 *  对一个hdfs或者s3的文件 注册成a.txt
 *  然后map中的open 可以广播到各个task
 * 可以在map中的map方法做 数据处理
 * 最后输出
 *
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/3/24 11:28
 */
public class DisCacheTest {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //注册一个文件，可以是hihdfs或者s3上的文件
        env.registerCachedFile("E:/intellj_space/FlinkTest/text","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        DataSet<String> map = data.map(new RichMapFunction<String, String>() {

           private ArrayList<String> alist = new ArrayList<String>();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2：使用文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> strings = FileUtils.readLines(myFile);
                for (String s:strings) {
                    alist.add(s);
                    System.out.println("分布式缓存为："+ s);

                }
            }

            @Override
            public String map(String value) throws Exception {
                //数据处理
                return alist+":"+value;
            }
        });

        map.print();


    }
}
