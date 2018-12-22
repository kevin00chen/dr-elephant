package com.red.bigdata.db;

/**
 * Created by chenkaiming on 2018/12/21.
 */
import com.mchange.v2.c3p0.DataSources;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.SQLException;

/**
 * Created by shengli on 11/6/15.
 */
public class ElephantDataSource implements Serializable {
  private static final Logger logger = Logger.getLogger(ElephantDataSource.class);

  private static volatile ElephantDataSource elephantDataSource = null;
  private DataSource dataSource;
  private static Object lockObj = new Object();

  public static ElephantDataSource getInstance() {
    if (elephantDataSource == null) {
      synchronized (lockObj) {
        elephantDataSource = new ElephantDataSource();
      }
    }
    return elephantDataSource;
  }

  private ElephantDataSource(){
    try {
      logger.info("Create DataBase Connection...");
      dataSource = DataSources.unpooledDataSource("jdbc:mysql://ckm.cfzo10akusv7.rds.cn-north-1.amazonaws.com.cn:3306/drelephant", "ckm", "AIvfcK2q9QubE");
    } catch (Exception e) {
      logger.error("Create DataBase Connection Failed!", e);
    }
  }

  public QueryRunner getQueryRunner() {
    return new QueryRunner(dataSource);
  }

  public void close() throws SQLException {
    dataSource.getConnection().close();
  }

}
