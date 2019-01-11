package com.red.bigdata.db;

import com.linkedin.drelephant.analysis.AnalyticJob;
import com.linkedin.drelephant.util.Utils;
import models.*;
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
            "(id, name, username, queue_name, start_time, finish_time, tracking_url, job_type, severity, score, workflow_depth, scheduler, job_name, job_exec_id, flow_exec_id, job_def_id, flow_def_id, job_exec_url, flow_exec_url, job_def_url, flow_def_url, resource_used, resource_wasted, total_delay, cluster) " +
            "values " +
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
                    appResult.totalDelay,
                    appResult.cluster
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

  public int[] upsertSparkTasks(Object[][] objects) throws SQLException {
    return queryRunner.batch("replace into spark_task(" +
            "app_id," +
            "job_id," +
            "stage_id," +
            "executor_id," +
            "task_id," +
            "attempt_id," +
            "launch_time," +
            "finish_time," +
            "duration," +
            "getting_result_time," +
            "status," +
            "accumulables," +
            "host," +
            "locality," +
            "error_message," +
            "executor_deser_time," +
            "executor_deser_cpu_time," +
            "executor_run_time," +
            "executor_cpu_time," +
            "result_size," +
            "jvm_gc_time," +
            "result_serial_time," +
            "memory_bytes_spilled," +
            "disk_bytes_spilled," +
            "peak_execution_memory," +
            "input_bytes_read," +
            "input_record_read," +
            "output_bytes_written," +
            "output_records_written," +
            "shuffle_remote_blocks_fetched," +
            "shuffle_local_blocks_fetched," +
            "shuffle_remote_bytes_read," +
            "shuffle_local_bytes_read," +
            "shuffle_fetch_wait_time," +
            "shuffle_records_read," +
            "shuffle_bytes_written," +
            "shuffle_records_written," +
            "shuffle_write_time," +
            "cluster) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", objects);
  }

  public int[] upsertSparkStorages(Object[][] objects) throws SQLException {
    return queryRunner.batch("replace into spark_storage(" +
                    "app_id," +
                    "executor_id," +
                    "host," +
                    "port," +
                    "blkmngr_id," +
                    "max_memory," +
                    "cache_size," +
                    "disk_used," +
                    "mem_used," +
                    "mem_remaining," +
                    "num_blocks," +
                    "num_rdd_blocks," +
                    "cluster" +
                    ") " +
                    "values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            objects);
  }

  public int[] upsertSparkExecutors(Object[][] objects) throws SQLException {
    return queryRunner.batch("replace into spark_executor(" +
            "app_id," +
            "executor_id," +
            "host_port," +
            "rdd_blocks," +
            "mem_used," +
            "max_mem," +
            "disk_used," +
            "completed_tasks," +
            "failed_tasks," +
            "total_tasks," +
            "duration," +
            "input_bytes," +
            "output_bytes," +
            "shuffle_read," +
            "shuffle_write," +
            "input_record," +
            "output_record," +
            "stdout," +
            "stderr," +
            "start_time," +
            "finish_time," +
            "finish_reason," +
            "cluster) " +
            "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", objects);
  }

  public Long upsertSparkEnv(SparkEnv env) throws SQLException {
    return queryRunner.insert(
            "replace into spark_env(" +
                    "app_id," +
                    "jvm_info," +
                    "spark_props," +
                    "sys_props," +
                    "class_path," +
                    "cluster" +
                    ") values (?,?,?,?,?,?)",
            new ScalarHandler<Long>("GENERATED_KEY"),
            new Object[]{
                    env.appId(),
                    env.jvmInfo(),
                    env.sparkProps(),
                    env.sysProps(),
                    env.classPath(),
                    env.cluster()
            });
  }

  public Long upsertSparkApp(SparkApp app) throws SQLException{
    return queryRunner.insert(
            "replace into spark_app(" +
                    "app_id," +
                    "app_name," +
                    "attempt_id," +
                    "queue," +
                    "tracking_url," +
                    "user," +
                    "spark_user," +
                    "vcore_seconds," +
                    "memory_seconds," +
                    "start_time," +
                    "end_time," +
                    "status," +
                    "view_acls," +
                    "admin_acls_groups, " +
                    "cluster" +
                    ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            new ScalarHandler<Long>("GENERATED_KEY"),
            new Object[]{app.appId(),
                    app.appName(),
                    app.attemptId(),
                    app.queue(),
                    app.trackingUrl(),
                    app.user(),
                    app.sparkUser(),
                    app.vcoreSeconds(),
                    app.memorySeconds(),
                    app.startTime(),
                    app.endTime(),
                    app.status(),
                    app.viewAcls(),
                    app.adminAclsGroups(),
                    app.cluster()
            });
  }


  public Long upsertSparkJob(SparkJob job) throws SQLException{
    return queryRunner.insert(
            "replace into spark_job(" +
                    "app_id," +
                    "job_id," +
                    "job_group," +
                    "stage_ids," +
                    "submission_time," +
                    "completed_time," +
                    "status," +
                    "error," +
                    "failed_stage_ids," +
                    "num_tasks," +
                    "num_active_tasks," +
                    "num_completed_tasks," +
                    "num_skipped_tasks," +
                    "num_failed_tasks," +
                    "num_killed_tasks," +
                    "task_failure_rate," +
                    "num_active_stages," +
                    "num_skipped_stages," +
                    "num_failed_stages," +
                    "cluster" +
                    ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            new ScalarHandler<Long>("GENERATED_KEY"),
            new Object[]{
                    job.appId(),
                    job.jobId(),
                    job.jobGroup(),
                    job.stageIds(),
                    job.submissionTime(),
                    job.completedTime(),
                    job.status(),
                    job.error(),
                    job.failedStageIds(),
                    job.numTasks(),
                    job.numActiveTasks(),
                    job.numCompletedTasks(),
                    job.numSkippedTasks(),
                    job.numFailedTasks(),
                    job.numKilledTasks(),
                    job.taskFailureRate(),
                    job.numActiveStages(),
                    job.numSkippedStages(),
                    job.numFailedStages(),
                    job.cluster()
            });
  }

  public Long upsertSparkStage(SparkStage stage) throws SQLException {
    return queryRunner.insert(
            "replace into spark_stage(" +
                    "app_id," +
                    "job_id," +
                    "stage_id," +
                    "attempt_id," +
                    "name," +
                    "parent_stage_id," +
                    "description," +
                    "accumulables," +
                    "status," +
                    "failure_reason," +
                    "num_active_tasks," +
                    "num_killed_tasks," +
                    "num_complete_tasks," +
                    "num_failed_tasks," +
                    "executor_run_time," +
                    "executor_cpu_time," +
                    "duration," +
                    "input_bytes," +
                    "input_records," +
                    "output_bytes," +
                    "output_records," +
                    "shuffle_read_bytes," +
                    "shuffle_read_records," +
                    "shuffle_write_bytes," +
                    "shuffle_write_records," +
                    "memory_bytes_spilled," +
                    "disk_bytes_spilled," +
                    "cluster" +
                    ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            new ScalarHandler<Long>("GENERATED_KEY"),
            new Object[]{
                    stage.appId(),
                    stage.jobId(),
                    stage.stageId(),
                    stage.attemptId(),
                    stage.name(),
                    stage.parentStageId(),
                    stage.description(),
                    stage.accumulables(),
                    stage.status(),
                    stage.failureReason(),
                    stage.numActiveTasks(),
                    stage.numKilledTasks(),
                    stage.numCompleteTasks(),
                    stage.numFailedTasks(),
                    stage.executorRunTime(),
                    stage.executorCpuTime(),
                    stage.duration(),
                    stage.inputBytes(),
                    stage.inputRecords(),
                    stage.outputBytes(),
                    stage.outputRecords(),
                    stage.shuffleReadBytes(),
                    stage.shuffleReadRecords(),
                    stage.shuffleWriteBytes(),
                    stage.shuffleWriteRecords(),
                    stage.memoryBytesSpilled(),
                    stage.diskBytesSpilled(),
                    stage.cluster()
            });
  }

  public void close() throws SQLException {
    ElephantDataSource.getInstance().close();
  }
}