package com.linkedin.drelephant;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.drelephant.analysis.*;
import com.linkedin.drelephant.security.HadoopSecurity;
import com.linkedin.drelephant.util.Utils;
import controllers.MetricsController;
import models.AppResult;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;
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
    _analyticJobGenerator = new AnalyticJobGeneratorHadoop2();
    try {
      Configuration conf = new Configuration();
      conf.set("yarn.resourcemanager.ha.enabled", "false");
      conf.set("yarn.resourcemanager.webapp.address", "ec2-52-80-160-71.cn-north-1.compute.amazonaws.com.cn:8088");
      conf.set("drelephant.analysis.fetch.initial.windowMillis", 1 * 60 * 60 * 1000 + "");
      _analyticJobGenerator.configure(conf);
    } catch (Exception e) {
      logger.error("Error occurred when configuring the analysis provider.", e);
      throw new RuntimeException(e);
    }
  }

//  public void run() {
//    logger.info("Dr.elephant has started");
//    try {
//      _hadoopSecurity = HadoopSecurity.getInstance();
//      _hadoopSecurity.doAs(new PrivilegedAction<Void>() {
//        @Override
//        public Void run() {
//          HDFSContext.load();
//          loadGeneralConfiguration();
//          loadAnalyticJobGenerator();
//          ElephantContext.init();
//
//          // Initialize the metrics registries.
//          MetricsController.init();
//
//          logger.info("executor num is " + _executorNum);
//          if (_executorNum < 1) {
//            throw new RuntimeException("Must have at least 1 worker thread.");
//          }
//          ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("dr-el-executor-thread-%d").build();
//          _threadPoolExecutor = new ThreadPoolExecutor(_executorNum, _executorNum, 0L, TimeUnit.MILLISECONDS,
//                  new LinkedBlockingQueue<Runnable>(), factory);
//
//          while (_running.get() && !Thread.currentThread().isInterrupted()) {
//            _analyticJobGenerator.updateResourceManagerAddresses();
//            lastRun = System.currentTimeMillis();
//
//            logger.info("Fetching analytic job list...");
//
//            try {
//              _hadoopSecurity.checkLogin();
//            } catch (IOException e) {
//              logger.info("Error with hadoop kerberos login", e);
//              //Wait for a while before retry
//              continue;
//            }
//
//            List<AnalyticJob> todos;
//            try {
//              todos = _analyticJobGenerator.fetchAnalyticJobs();
//            } catch (Exception e) {
//              logger.error("Error fetching job list. Try again later...", e);
//              //Wait for a while before retry
//              continue;
//            }
//
//            for (AnalyticJob analyticJob : todos) {
//              _threadPoolExecutor.submit(new ExecutorJob(analyticJob));
//            }
//
//            int queueSize = _threadPoolExecutor.getQueue().size();
//            MetricsController.setQueueSize(queueSize);
//            logger.info("Job queue size is " + queueSize);
//
//            //Wait for a while before next fetch
//            waitInterval(_fetchInterval);
//          }
//          logger.info("Main thread is terminated.");
//          return null;
//        }
//      });
//    } catch (Exception e) {
//      logger.error(e.getMessage());
//      logger.error(ExceptionUtils.getStackTrace(e));
//    }
//  }

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

    private void jobFate () {
      if (_analyticJob != null && _analyticJob.retry()) {
        logger.warn("Add analytic job id [" + _analyticJob.getAppId() + "] into the retry list.");
        _analyticJobGenerator.addIntoRetries(_analyticJob);
      } else if (_analyticJob != null && _analyticJob.isSecondPhaseRetry()) {
        //Putting the job into a second retry queue which fetches jobs after some interval. Some spark jobs may need more time than usual to process, hence the queue.
        logger.warn("Add analytic job id [" + _analyticJob.getAppId() + "] into the second retry list.");
        _analyticJobGenerator.addIntoSecondRetryQueue(_analyticJob);
      } else {
        if (_analyticJob != null) {
          MetricsController.markSkippedJob();
          logger.error("Drop the analytic job. Reason: reached the max retries for application id = ["
                  + _analyticJob.getAppId() + "].");
        }
      }
    }
  }



  public static void main(String[] args) throws Exception {
    TestElephantFetcher test = new TestElephantFetcher();
    test.f1();

    System.out.println("===");
  }

  public void f1() throws Exception {
//    loadGeneralConfiguration();
    loadAnalyticJobGenerator();

    _analyticJobGenerator.updateResourceManagerAddresses();
    lastRun = System.currentTimeMillis();

    logger.info("Fetching analytic job list...");

    List<AnalyticJob> todos;
    todos = _analyticJobGenerator.fetchAnalyticJobs();

    for (AnalyticJob analyticJob : todos) {
      System.out.println("=====");

      ExecutorJob executorJob = new ExecutorJob(analyticJob);
      executorJob.run();

    }

  }

}
