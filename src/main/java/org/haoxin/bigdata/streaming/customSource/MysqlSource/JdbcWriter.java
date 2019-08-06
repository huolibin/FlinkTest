package org.haoxin.bigdata.streaming.customSource.MysqlSource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.haoxin.bigdata.utils.config.ConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/17 11:14
 */
public class JdbcWriter extends RichSinkFunction<Tuple2<String,String>> {

    //记录日志
    private  static final Logger logger = LoggerFactory.getLogger(JdbcWriter.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(ConfigKeys.DRIVER_CLASS);
        connection = DriverManager.getConnection(ConfigKeys.SOURCE_DRIVER_URL, ConfigKeys.SOURCE_USER, ConfigKeys.SOURCE_PASSWORD);
        ps = connection.prepareCall(ConfigKeys.INSERT_SQL);
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }catch (Exception e){
            logger.error("close:{}",e);
        }

    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        try {
            String name = value.f0;
            ps.setString(1,name);
            ps.executeUpdate();
        } catch (Exception e){
            logger.error("invoke{}",e);
        }

    }
}
