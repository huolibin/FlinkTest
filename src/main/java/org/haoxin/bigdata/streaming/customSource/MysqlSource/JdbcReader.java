package org.haoxin.bigdata.streaming.customSource.MysqlSource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.haoxin.bigdata.utils.config.ConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2019/7/17 11:14
 */
public class JdbcReader extends RichParallelSourceFunction<Tuple2<String,String>> {

    //记录日志
    private  static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(ConfigKeys.DRIVER_CLASS);
        connection = DriverManager.getConnection(ConfigKeys.SOURCE_DRIVER_URL, ConfigKeys.SOURCE_USER, ConfigKeys.SOURCE_PASSWORD);
        ps = connection.prepareCall(ConfigKeys.SOURCE_SQL);
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
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        try {
            while (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String name = resultSet.getString("name");
                    String sex = resultSet.getString("sex");
                    Tuple2<String, String> tuple2 = new Tuple2<>();
                    tuple2.setFields(name, sex);
                    sourceContext.collect(tuple2);
                    Thread.sleep(5000*6);
                }
            }
        }catch (Exception e){
            logger.error("runException:{}",e);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;

    }
}
