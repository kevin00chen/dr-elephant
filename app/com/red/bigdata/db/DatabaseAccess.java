package com.red.bigdata.db;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.util.Utils;
import models.AppHeuristicResult;
import models.AppResult;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenkaiming on 2018/12/21.
 */
public class DatabaseAccess implements Serializable {
  private static final Logger logger = Logger.getLogger(DatabaseAccess.class);
  private static volatile DatabaseAccess mrDatabaseAccess = null;
  private final QueryRunner queryRunner = ElephantDataSource.getInstance().getQueryRunner();
  private static Object lockObj = new Object();

  public static DatabaseAccess getInstance() {
    if (mrDatabaseAccess == null) {
      synchronized (lockObj) {
        mrDatabaseAccess = new DatabaseAccess();
      }
    }
    return mrDatabaseAccess;
  }


  public int[] upsertYarnAppHeuristicResultDetails(Object[][] objects) throws SQLException {
    String sql = "replace into yarn_app_heuristic_result_details(yarn_app_heuristic_result_id, name, value, details) " +
            "values (?,?,?,?)";
    logger.debug("Replace Into yarn_app_heuristic_result_details ===>\n" + sql);
    return queryRunner.batch(sql,
            objects);
  }

  public Long upsetYarnAppHeuristicResult(AppHeuristicResult yarnAppHeuristicResult, String appId) throws SQLException {
    String sql = "replace into yarn_app_heuristic_result(yarn_app_result_id, heuristic_class, heuristic_name, severity, score) " +
            "values (?,?,?,?,?)";
    logger.debug("Replace Into yarn_app_heuristic_result ===>\n" + sql);
    return queryRunner.insert(
            sql,
            new ScalarHandler<Long>("GENERATED_KEY"),
            new Object[]{appId,
                    yarnAppHeuristicResult.heuristicClass,
                    yarnAppHeuristicResult.heuristicName,
                    yarnAppHeuristicResult.severity.getValue(),
                    yarnAppHeuristicResult.score
            });
  }


  public Long upsetYarnAppResult(AppResult appResult) throws SQLException {
    String sql = "replace into yarn_app_result" +
            "(id, name, username, queue_name, start_time, finish_time, tracking_url, job_type, severity, score, workflow_depth, scheduler, job_name, job_exec_id, flow_exec_id, job_def_id, flow_def_id, job_exec_url, flow_exec_url, job_def_url, flow_def_url, resource_used, resource_wasted, total_delay) " +
            "values " +
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    logger.debug("Replace Into yarn_app_result ===>\n" + sql);
    return queryRunner.insert(
            sql,
            new ScalarHandler<Long>("GENERATED_KEY"),
            new Object[] {
                    appResult.id,
                    appResult.name,
                    appResult.username,
                    appResult.queueName,
                    appResult.startTime,
                    appResult.finishTime,
                    appResult.trackingUrl,
                    appResult.jobType,
                    appResult.severity.getValue(),
                    appResult.score,
                    appResult.workflowDepth,
                    appResult.scheduler,
                    appResult.jobName,
                    appResult.jobExecId,
                    appResult.flowExecId,
                    appResult.jobDefId,
                    appResult.flowDefId,
                    appResult.jobExecUrl,
                    appResult.flowExecUrl,
                    appResult.jobDefUrl,
                    appResult.flowDefUrl,
                    appResult.resourceUsed,
                    appResult.resourceWasted,
                    appResult.totalDelay
            });
  }


  public Long upsertYarnAppOriginal(List<AnalyticJob> appList) throws SQLException {
    List<Object[]> objectsList = new ArrayList<Object[]>(appList.size());
    for (AnalyticJob app : appList) {
      objectsList.add(new Object[] {
              app.getAppId(),
              Utils.truncateField(app.getName(),1000, app.getAppId()),
              app.getQueueName(),
              app.getUser(),
              app.getState(),
              app.getFinalStatus(),
              app.getAppType().getName(),
              app.getApplicationTags(),
              app.getTrackingUrl(),
              app.getStartTime(),
              app.getFinishTime(),
              app.getElapsedTime(),
              app.getMemorySeconds(),
              app.getVcoreSeconds(),
              app.getDiagnostics(),
              app.getClusterName()
      });
    }

    Object[][] objectArr = objectsList.toArray(new Object[0][0]);

    String sql = "replace into yarn_app_original(" +
            "app_id, " +
            "name, " +
            "queue_name, " +
            "user, " +
            "state, " +
            "final_status, " +
            "application_type, " +
            "application_tags, " +
            "tracking_url, " +
            "start_time, " +
            "finish_time, " +
            "elapsed_time, " +
            "memory_seconds, " +
            "vcore_seconds, " +
            "diagnostics, " +
            "cluster_name) " +
            "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
    queryRunner.batch(sql, objectArr);
    return null;
  }




  public void close() throws SQLException {
    ElephantDataSource.getInstance().close();
  }
}