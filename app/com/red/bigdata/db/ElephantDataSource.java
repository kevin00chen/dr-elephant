package com.red.bigdata.db;

/**
 * Created by chenkaiming on 2018/12/21.
 */
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.SQLException;

/**
 * Created by shengli on 11/6/15.
 */
public class ElephantDataSource implements Serializable {

  private ElephantDataSource(){}

  private static class DataSourceHolder {
    public static final DataSource dataSource = new ComboPooledDataSource();
  }

  private static class QueryRunnerHolder{
    public static final QueryRunner queryRunner = new QueryRunner(getDataSource());
  }

  public static final DataSource getDataSource() {
    return DataSourceHolder.dataSource;
  }

  public static final QueryRunner getQueryRunner() {
    return QueryRunnerHolder.queryRunner;
  }

  public static final void close() throws SQLException {
    DataSourceHolder.dataSource.getConnection().close();
  }

}
