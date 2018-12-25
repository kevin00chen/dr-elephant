package com.red.bigdata.service

/**
  * Created by chenkaiming on 2018/12/21.
  */

import java.sql.Timestamp

import com.linkedin.drelephant.analysis.AnalyticJob
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.tez.data.TezApplicationData
import com.red.bigdata.db.DatabaseAccess
import com.red.bigdata.util.MonitorConstants
import models._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.Logger
import org.apache.spark.deploy.history.SparkDataCollection
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**
  * Created by chenkaiming on 2017/6/29.
  */
class DBService(dao: DatabaseAccess) {
  val logger = Logger.getLogger(this.getClass)

  /**
    * 任务经启发式算法分析后入库
    * 主要涉及三张表:
    * yarn_app_result
    * yarn_app_heuristic_result
    * yarn_app_heuristic_result_details
    *
    * @param appResult
    */
  def saveYarnAppResult(appResult: AppResult) = {
    try {
      // 保存 yarn_app_result
      dao.upsetYarnAppResult(appResult)

      val heuristicResultList = appResult.yarnAppHeuristicResults
      // 遍历保存yarn_app_heuristic_result, 返回的id赋值给 yarn_app_heuristic_result_details 的 yarn_app_heuristic_result_id字段
      for (heuristicResult: AppHeuristicResult <- heuristicResultList) {
        // 直接获取appResult对象的id属性(即jobid),赋值给传给yarn_app_heuristic_result表的yarn_app_result_id字段
        val id = dao.upsetYarnAppHeuristicResult(heuristicResult, appResult.id)

        val heuristicResultDetailList = heuristicResult.yarnAppHeuristicResultDetails

        val tasks = new ArrayBuffer[Array[Any]]
        for (heuristicResultDetail: AppHeuristicResultDetails <- heuristicResultDetailList) {
          val task = new ArrayBuffer[Any]
          task.++=(Array(id, heuristicResultDetail.name, heuristicResultDetail.value, heuristicResultDetail.details))
          tasks += task.toArray
        }
        dao.upsertYarnAppHeuristicResultDetails(tasks.toArray.asInstanceOf[Array[Array[AnyRef]]])
        tasks.clear()
      }
    } catch {
      case e: Exception => logger.error("Error thrown when saving AppResult : " + ExceptionUtils.getStackTrace(e))
    }
  }

  /**
    * 保存Yarn页面上的Application原始信息
    *
    * @param yarnAppOrigins
    * @return
    */
  def saveYarnAppOriginal(yarnAppOrigins: java.util.List[AnalyticJob]) = {
    try {
      dao.upsertYarnAppOriginal(yarnAppOrigins)
    } catch {
      case e: Exception ⇒ logger.error("Error thrown when saving YarnAppOrginal ")
    }
  }

  /**
    * 根据类型保存任务明细数据
    *
    * @param analyticJob
    */
  def saveDataDetail(analyticJob: AnalyticJob)  = {
    val dataDetail = analyticJob.getDataDetail
    logger.info(s"xxxxxxxxxxxxxxxxxxxxx: saveDataDetail ${analyticJob.getAppId}")
    logger.info(s"${analyticJob.getAppType.getName}, ${dataDetail.getClass}")
    // Spark任务
    if (dataDetail.isInstanceOf[SparkApplicationData]) {
      logger.info(s"==========Save Spark Job Detail Data app_id: ${analyticJob.getAppId}")
      val sparkApplicationData = dataDetail.asInstanceOf[SparkApplicationData]
      val sparkModels = sparkApplicationData.sparkBaseModels

      if (!sparkModels.isEmpty) {
        saveSparkDetailInfosToDB(sparkModels)
      }
//      // environment明细
//      addSparkEnv(analyticJob, sparkDataCollection, sparkModels)
//      // spark app明细
//      addSparkApp(analyticJob, sparkDataCollection, sparkModels)
//      // executor明细
//      addSparkExecutors(analyticJob, sparkDataCollection, sparkModels)
//      // jobs明细
//      addSparkJobs(analyticJob, sparkDataCollection, sparkModels)
//      // stages明细
//      addSparkStages(analyticJob, sparkDataCollection, sparkModels)
//      // tasks明细
//      addSparkTasks(analyticJob, sparkDataCollection, sparkModels)
//      // storages明细
//      addSparkStorages(analyticJob, sparkDataCollection, sparkModels)




    }
    // MR 任务
    else if (dataDetail.isInstanceOf[MapReduceApplicationData]) {

    }
    // TEZ任务
    else if (dataDetail.isInstanceOf[TezApplicationData]) {

    }
  }

  private def saveSparkDetailInfosToDB(detailInfos: List[BaseModel]) = {
    if (!detailInfos.isEmpty) {
      val sparkTasks = new ArrayBuffer[Array[Any]]
      val sparkStorages = new ArrayBuffer[Array[Any]]
      val sparkExecutors = new ArrayBuffer[Array[Any]]

      detailInfos.foreach { detailInfo =>
        if (detailInfo.isInstanceOf[SparkApp]) {
          try {
            dao.upsertSparkApp(detailInfo.asInstanceOf[SparkApp])
          } catch {
            case e: Exception => println("Error thrown when processing sparkAppSummary : " + ExceptionUtils.getStackTrace(e))
          }
        } else if (detailInfo.isInstanceOf[SparkEnv]) {
          try {
            dao.upsertSparkEnv(detailInfo.asInstanceOf[SparkEnv])
          } catch {
            case e: Exception => println("Error throw when upsert Spark Environment Data : " + ExceptionUtils.getStackTrace(e))
          }
        } else if (detailInfo.isInstanceOf[SparkStorage]) {
          try {
            val sparkStorage = new ArrayBuffer[Any]
            val value = detailInfo.asInstanceOf[SparkStorage]
            sparkStorage.++=(Array(
              value.appId,
              value.executorId,
              value.host,
              value.port,
              value.blkmngrId,
              value.maxMemory,
              value.cacheSize,
              value.diskUsed,
              value.memUsed,
              value.memRemaining,
              value.numBlocks,
              value.numRddBlocks,
              value.cluster
            ))
            sparkStorages += sparkStorage.toArray
            if (sparkStorages.size == MonitorConstants.BATCH_INSERT_SIZE) {
              dao.upsertSparkStorages(sparkStorages.toArray.asInstanceOf[Array[Array[AnyRef]]])
              sparkStorages.clear()
            }
          } catch {
            case e: Exception => println("Error thrown when upsert Spark Storage Infos : " + ExceptionUtils.getStackTrace(e))
          }
        } else if (detailInfo.isInstanceOf[SparkExecutor]) {
          try {
            val executor = new ArrayBuffer[Any]
            val value = detailInfo.asInstanceOf[SparkExecutor]
            executor.++=(Array(
              value.appId,
              value.executorId,
              value.hostPort,
              value.rddBlocks,
              value.memUsed,
              value.maxMem,
              value.diskUsed,
              value.completedTasks,
              value.failedTasks,
              value.totalTasks,
              value.duration,
              value.inputBytes,
              value.outputBytes,
              value.shuffleRead,
              value.shuffleWrite,
              value.inputRecord,
              value.outputRecord,
              value.stdout,
              value.stderr,
              value.startTime,
              value.finishTime,
              value.finishReason,
              value.cluster
            ))
            sparkExecutors += executor.toArray
            if (sparkExecutors.size == MonitorConstants.BATCH_INSERT_SIZE) {
              dao.upsertSparkExecutors(sparkExecutors.toArray.asInstanceOf[Array[Array[AnyRef]]])
              sparkExecutors.clear()
            }
          } catch {
            case e: Exception => println("Error thrown when upsert Spark Executor Infos : " + ExceptionUtils.getStackTrace(e))
          }
        } else if (detailInfo.isInstanceOf[SparkJob]) {
          try {
            dao.upsertSparkJob(detailInfo.asInstanceOf[SparkJob])
          } catch {
            case e: Exception => println("Error thrown when upsert Spark Job Infos : " + ExceptionUtils.getStackTrace(e))
          }
        } else if (detailInfo.isInstanceOf[SparkStage]) {
          try {
            dao.upsertSparkStage(detailInfo.asInstanceOf[SparkStage])
          } catch {
            case e: Exception => println("Error thrown when upsert Spark Stage Infos : " + ExceptionUtils.getStackTrace(e))
          }
        } else if (detailInfo.isInstanceOf[SparkTask]) {
          try {
            if (detailInfo.asInstanceOf[SparkTask] != null) {
              val task = new ArrayBuffer[Any]
              val value = detailInfo.asInstanceOf[SparkTask]
              task.++=(Array(
                value.appId,
                value.jobId,
                value.stageId,
                value.executorId,
                value.taskId,
                value.attemptId,
                value.launchTime,
                value.finishTime,
                value.duration,
                value.gettingResultTime,
                value.status,
                value.accumulables,
                value.host,
                value.locality,
                value.errorMessage,
                value.executorDeserTime,
                value.executorDeserCpuTime,
                value.executorRunTime,
                value.executorCpuTime,
                value.resultSize,
                value.jvmGCTime,
                value.resultSerialTime,
                value.memoryBytesSpilled,
                value.diskBytesSpilled,
                value.peakExecutionMemory,
                value.inputBytesRead,
                value.inputRecordRead,
                value.outputBytesWritten,
                value.outputRecordsWritten,
                value.shuffleRemoteBlocksFetched,
                value.shuffleLocalBlocksFetched,
                value.shuffleRemoteBytesRead,
                value.shuffleLocalBytesRead,
                value.shuffleFetchWaitTime,
                value.shuffleRecordsRead,
                value.shuffleBytesWritten,
                value.shuffleRecordsWritten,
                value.shuffleWriteTime,
                value.cluster
              ))
              sparkTasks += task.toArray
              if (sparkTasks.size == MonitorConstants.BATCH_INSERT_SIZE) {
                dao.upsertSparkTasks(sparkTasks.toArray.asInstanceOf[Array[Array[AnyRef]]])
                sparkTasks.clear()
              }
            }
          } catch {
            case e: Exception =>  println("Error thrown when processing sparkTaskSummary : " + ExceptionUtils.getStackTrace(e))
          }
        }
      }

      if (sparkStorages.size > 0) {
        try {
          dao.upsertSparkStorages(sparkStorages.toArray.asInstanceOf[Array[Array[AnyRef]]])
        } catch {
          case e: Exception => logger.info("upsert spark_storage error: " + e.getStackTrace)
        }
      }

      if (sparkTasks.size > 0) {
        try {
          dao.upsertSparkTasks(sparkTasks.toArray.asInstanceOf[Array[Array[AnyRef]]])
        } catch {
          case e: Exception => logger.info("upsert spark_task error: " + e.getStackTrace)
        }
      }

      if (sparkExecutors.size > 0) {
        try {
          dao.upsertSparkExecutors(sparkExecutors.toArray.asInstanceOf[Array[Array[AnyRef]]])
        } catch {
          case e: Exception => logger.info("upsert spark_executor error: " + e.getStackTrace)
        }
      }
    }
  }

//  private def addSparkStorages(analyticJob: AnalyticJob, sparkDataCollection: SparkDataCollection, listBuffer: ListBuffer[BaseModel]): Unit = {
//    sparkDataCollection.getStorageData.getStorageStatusList.asScala.foreach { storage =>
//      val blkMngrId = storage.blockManagerId
//      listBuffer += SparkStorage(analyticJob.getAppId,
//        blkMngrId.executorId,
//        blkMngrId.host,
//        blkMngrId.port,
//        blkMngrId.toString(),
//        storage.maxMem,
//        storage.cacheSize,
//        storage.diskUsed,
//        storage.memUsed,
//        storage.memRemaining,
//        storage.numBlocks,
//        storage.numRddBlocks,
//        analyticJob.getClusterName)
//    }
//  }
//
//  private def addSparkStages(analyticJob: AnalyticJob, sparkDataCollection: SparkDataCollection, listBuffer: ListBuffer[BaseModel]): Unit = {
//    val sparkJobStageData = sparkDataCollection.getJobProgressData
//    sparkJobStageData.getStageIdToInfo.values.asScala.foreach { stage =>
//      listBuffer += SparkStage(analyticJob.getAppId,
//        stage.jobId,
//        stage.stageId,
//        stage.attemptId,
//        stage.parentIds,
//        StringEscapeUtils.escapeJava(stage.name),
//        StringEscapeUtils.escapeJava(stage.description),
//        stage.accumulables,
//        stage.status,
//        stage.failureReason,
//        stage.numActiveTasks,
//        stage.numKilledTasks,
//        stage.numCompleteTasks,
//        stage.numFailedTasks,
//        stage.executorRunTime,
//        stage.executorCpuTime,
//        stage.duration,
//        stage.inputBytes,
//        stage.inputRecords,
//        stage.outputBytes,
//        stage.outputRecords,
//        stage.shuffleReadBytes,
//        stage.shuffleReadRecords,
//        stage.shuffleWriteBytes,
//        stage.shuffleWriteRecords,
//        stage.memoryBytesSpilled,
//        stage.diskBytesSpilled,
//        analyticJob.getClusterName)
//    }
//  }
//
//
//  private def addSparkTasks(analyticJob: AnalyticJob, sparkDataCollection: SparkDataCollection, listBuffer: ListBuffer[BaseModel]): Unit = {
//    val sparkJobStageData = sparkDataCollection.getJobProgressData
//    sparkJobStageData.getTaskIdToInfo.values.asScala.foreach { task =>
//      listBuffer += SparkTask(analyticJob.getAppId,
//        task.jobId,
//        task.stageId,
//        task.executorId,
//        task.taskId,
//        task.attemptId,
//        new Timestamp(task.launchTime),
//        new Timestamp(task.finishTime),
//        task.duration,
//        task.gettingResultTime,
//        task.status,
//        task.accumulables,
//        task.host,
//        task.locality,
//        task.errorMessage,
//        task.executorDeserTime,
//        task.executorDeserCpuTime,
//        task.executorRunTime,
//        task.executorCpuTime,
//        task.resultSize,
//        task.jvmGCTime,
//        task.resultSerialTime,
//        task.memoryBytesSpilled,
//        task.diskBytesSpilled,
//        task.peakExecutionMemory,
//        task.inputBytesRead,
//        task.inputRecordRead,
//        task.outputBytesWritten,
//        task.outputRecordsWritten,
//        task.shuffleRemoteBlocksFetched,
//        task.shuffleLocalBlocksFetched,
//        task.shuffleRemoteBytesRead,
//        task.shuffleLocalBytesRead,
//        task.shuffleFetchWaitTime,
//        task.shuffleRecordsRead,
//        task.shuffleBytesWritten,
//        task.shuffleRecordsWritten,
//        task.shuffleWriteTime,
//        analyticJob.getClusterName)
//    }
//  }
//
//  private def addSparkJobs(analyticJob: AnalyticJob, sparkDataCollection: SparkDataCollection, listBuffer: ListBuffer[BaseModel]): Unit = {
//    val sparkJobStageData = sparkDataCollection.getJobProgressData
//    sparkJobStageData.getJobIdToInfo.values.asScala.foreach { job =>
//      listBuffer += SparkJob(analyticJob.getAppId,
//        job.jobId,
//        job.jobGroup,
//        job.stageIds.asScala.mkString(","),
//        new Timestamp(job.startTime),
//        new Timestamp(job.endTime),
//        job.status,
//        job.error,
//        job.failedStageIds,
//        job.numTasks,
//        job.numActiveTasks,
//        job.numCompletedTasks,
//        job.numSkippedTasks,
//        job.numFailedTasks,
//        job.numKilledTasks,
//        job.getFailureRate,
//        job.numActiveStages,
//        job.numSkippedStages,
//        job.numFailedStages,
//        analyticJob.getClusterName)
//    }
//  }
//
//  private def addSparkExecutors(analyticJob: AnalyticJob, sparkDataCollection: SparkDataCollection, listBuffer: ListBuffer[BaseModel]): Unit = {
//    val sparkExecutorData = sparkDataCollection.getExecutorData()
//    sparkExecutorData.getExecutors.asScala.foreach { executorId =>
//      val executorInfo = sparkExecutorData.getExecutorInfo(executorId)
//      listBuffer += SparkExecutor(analyticJob.getAppId,
//        executorInfo.execId,
//        executorInfo.hostPort,
//        executorInfo.rddBlocks,
//        executorInfo.memUsed,
//        executorInfo.maxMem,
//        executorInfo.diskUsed,
//        executorInfo.completedTasks,
//        executorInfo.failedTasks,
//        executorInfo.totalTasks,
//        executorInfo.duration,
//        executorInfo.inputBytes,
//        executorInfo.outputBytes,
//        executorInfo.shuffleRead,
//        executorInfo.shuffleWrite,
//        executorInfo.inputRecord,
//        executorInfo.outputRecord,
//        executorInfo.stdout,
//        executorInfo.stderr,
//        new Timestamp(executorInfo.startTime),
//        new Timestamp(executorInfo.finishTime),
//        executorInfo.finishReason,
//        analyticJob.getClusterName)
//    }
//  }
//
//  private def addSparkEnv(analyticJob: AnalyticJob, sparkDataCollection: SparkDataCollection, listBuffer: ListBuffer[BaseModel]): Unit = {
//    val env = sparkDataCollection.getEnvironmentData()
//    listBuffer += SparkEnv(analyticJob.getAppId,
//      StringEscapeUtils.escapeJava(env.getJVMInformations.toString),
//      StringEscapeUtils.escapeJava(env.getSparkProperties.toString),
//      StringEscapeUtils.escapeJava(env.getSystemProperties.toString),
//      StringEscapeUtils.escapeJava(env.getClassPathEntries.toString),
//      analyticJob.getClusterName)
//  }
//
//  private def addSparkApp(analyticJob: AnalyticJob, sparkDataCollection: SparkDataCollection, listBuffer: ListBuffer[BaseModel]) = {
//    val generalData = sparkDataCollection.getGeneralData()
//    val viewAcls =
//      if (generalData.getViewAcls == null || generalData.getViewAcls.isEmpty) ""
//      else generalData.getViewAcls.asScala.mkString(",")
//    val adminAcls =
//      if (generalData.getAdminAcls == null || generalData.getAdminAcls.isEmpty) ""
//      else generalData.getAdminAcls.asScala.mkString(",")
//
//    listBuffer += SparkApp(analyticJob.getAppId,
//      generalData.getApplicationName,
//      generalData.getAttemptId,
//      analyticJob.getQueueName,
//      analyticJob.getTrackingUrl,
//      analyticJob.getUser,
//      generalData.getSparkUser,
//      analyticJob.getVcoreSeconds,
//      analyticJob.getMemorySeconds,
//      new Timestamp(generalData.getStartTime),
//      new Timestamp(generalData.getEndTime),
//      analyticJob.getFinalStatus,
//      viewAcls,
//      adminAcls,
//      analyticJob.getClusterName)
//  }


}