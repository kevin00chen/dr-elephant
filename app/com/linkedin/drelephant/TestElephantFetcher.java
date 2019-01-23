package com.linkedin.drelephant;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.drelephant.analysis.*;
import com.linkedin.drelephant.configurations.cluster.ClusterConfiguration;
import com.linkedin.drelephant.configurations.cluster.ClusterConfigurationData;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.spark.fetchers.FSFetcher;
import com.linkedin.drelephant.util.InfoExtractor;
import com.linkedin.drelephant.util.Utils;
import controllers.MetricsController;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by chenkaiming on 2018/12/20.
 */
public class TestElephantFetcher {

  private static final Logger logger = Logger.getLogger(ElephantRunner.class);

  private static final long FETCH_INTERVAL = 60 * 1000;     // Interval between fetches
  private static final long RETRY_INTERVAL = 60 * 1000;     // Interval between retries

  private long lastRun;

  Map<String, Map<String, String>> _paramsToCluster = new HashMap<String, Map<String, String>>();
//  private long _fetchInterval;
//  private long _retryInterval;
//  private int _executorNum;
//  private HadoopSecurity _hadoopSecurity;
//  private ThreadPoolExecutor _threadPoolExecutor;
  private AnalyticJobGenerator _analyticJobGenerator;

//  private void loadGeneralConfiguration() {
//    _executorNum = 3;
//    _fetchInterval = FETCH_INTERVAL;
//    _retryInterval = RETRY_INTERVAL;
//  }

  private void loadAnalyticJobGenerator() {
    Document document = Utils.loadXMLDoc("/Users/chenkaiming/files/workspace/apache/dr-elephant/app-conf/ClusterConf.xml");
    ClusterConfiguration clusterConf = new ClusterConfiguration(document.getDocumentElement());

    for (ClusterConfigurationData confData : clusterConf.getClustersConfDataList()) {
      _paramsToCluster.put(confData.getClusterName(), confData.getClusterParams());
    }
    _analyticJobGenerator = new AnalyticJobGeneratorHadoop2(_paramsToCluster);
    try {
      Configuration conf = new Configuration();
      conf.set("yarn.resourcemanager.ha.enabled", "false");
      conf.set("yarn.resourcemanager.webapp.address", "ec2-54-222-151-203.cn-north-1.compute.amazonaws.com.cn:8088");
      conf.set("drelephant.analysis.fetch.initial.windowMillis", 1 * 60 * 60 * 1000 + "");
      _analyticJobGenerator.configure(conf);
    } catch (Exception e) {
      logger.error("Error occurred when configuring the analysis provider.", e);
      throw new RuntimeException(e);
    }
  }
  private class ExecutorJob {

    private AnalyticJob _analyticJob;

    ExecutorJob(AnalyticJob analyticJob) {
      _analyticJob = analyticJob;
    }

    public void run() {
      try {
        String analysisName = String.format("%s %s", _analyticJob.getAppType().getName(), _analyticJob.getAppId());
        long analysisStartTimeMillis = System.currentTimeMillis();
        logger.info(String.format("Analyzing %s", analysisName));
        AppResult result = _analyticJob.getAnalysis();
        result.save();
        long processingTime = System.currentTimeMillis() - analysisStartTimeMillis;
        logger.info(String.format("Analysis of %s took %sms", analysisName, processingTime));
        MetricsController.setJobProcessingTime(processingTime);
        MetricsController.markProcessedJobs();

      } catch (Exception e) {
        logger.info("Thread interrupted");
        logger.info(e.getMessage());
        logger.info(ExceptionUtils.getStackTrace(e));
      }
    }
  }



  public static void main(String[] args) throws Exception {
    TestElephantFetcher test = new TestElephantFetcher();
    List<AnalyticJob> todos = test.fetchAnalyticJob();

    test.analyzeSparkJob(todos);

    System.out.println("===");
  }

  /**
   * 从eventlog中解析Spark任务
   * @throws Exception
   */
  public void analyzeSparkJob(List<AnalyticJob> todos) throws Exception {
    List<AnalyticJob> sparkJobs = new ArrayList<AnalyticJob>();
    List<AnalyticJob> mrJobs = new ArrayList<AnalyticJob>();
    List<AnalyticJob> tezJobs = new ArrayList<AnalyticJob>();
    for (AnalyticJob analyticJob : todos) {
      String type = analyticJob.getAppType().getName();
      System.out.println("type = " + type);
      if (type.equals("SPARK")) {
        sparkJobs.add(analyticJob);
      } else if (type.equals("MAPREDUCE")) {
        mrJobs.add(analyticJob);
      } else if (type.equals("TEZ")) {
        tezJobs.add(analyticJob);
      }
    }

//    for (AnalyticJob analyticJob : sparkJobs) {
//      System.out.println("获取Spark任务数据");
//
//      ExecutorJob executorJob = new ExecutorJob(analyticJob);
//      executorJob.run();
//    }

//    for (AnalyticJob analyticJob : mrJobs) {
//      System.out.println("获取MR任务数据");
//      ExecutorJob executorJob = new ExecutorJob(analyticJob);
//      executorJob.run();
//    }

    for (AnalyticJob analyticJob : tezJobs) {
      System.out.println("获取TEZ任务数据");

      ExecutorJob executorJob = new ExecutorJob(analyticJob);
      executorJob.run();
    }

    System.out.println();

//    sparkJob.set
  }


  /**
   * 定期访问YARN RM获取任务列表
   * @throws Exception
   */
  public List<AnalyticJob> fetchAnalyticJob() throws Exception {
    loadAnalyticJobGenerator();

    _analyticJobGenerator.updateResourceManagerAddresses();
    lastRun = System.currentTimeMillis();

    logger.info("Fetching analytic job list...");

    List<AnalyticJob> todos = new ArrayList<AnalyticJob>();

    for (String clusterName: _paramsToCluster.keySet()) {
      todos.addAll(_analyticJobGenerator.fetchAnalyticJobs(clusterName));
    }

//    todos = _analyticJobGenerator.fetchAnalyticJobs();

    return todos;
  }

}
