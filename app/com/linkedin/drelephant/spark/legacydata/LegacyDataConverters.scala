/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.legacydata

import java.sql.Timestamp
import java.util.Date

import com.linkedin.drelephant.analysis.AnalyticJob
import models._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.deploy.history.SparkDataCollection

import scala.collection.JavaConverters
import scala.collection.mutable.ListBuffer
import scala.util.Try

import com.linkedin.drelephant.spark.fetchers.statusapiv1._
import org.apache.spark.JobExecutionStatus
import com.linkedin.drelephant.spark.fetchers.statusapiv1.StageStatus

/**
  * Converters for legacy SparkApplicationData to current SparkApplicationData.
  *
  * The converters make a best effort, providing default values for attributes the legacy data doesn't provide.
  * In practice, the Dr. Elephant Spark heuristics end up using a relatively small subset of the converted data.
  */
object LegacyDataConverters {
  import JavaConverters._

  def convert(analyticJob: AnalyticJob, legacyData: SparkApplicationData): com.linkedin.drelephant.spark.data.SparkApplicationData = {
    com.linkedin.drelephant.spark.data.SparkApplicationData(
      legacyData.getAppId,
      extractAppConfigurationProperties(legacyData),
      extractApplicationInfo(legacyData),
      extractJobDatas(legacyData),
      extractStageDatas(legacyData),
      extractExecutorSummaries(legacyData),
      extractBaseModels(analyticJob, legacyData)
    )
  }

  def convert(legacyData: SparkApplicationData): com.linkedin.drelephant.spark.data.SparkApplicationData = {
    com.linkedin.drelephant.spark.data.SparkApplicationData(
      legacyData.getAppId,
      extractAppConfigurationProperties(legacyData),
      extractApplicationInfo(legacyData),
      extractJobDatas(legacyData),
      extractStageDatas(legacyData),
      extractExecutorSummaries(legacyData),
      List.empty
    )
  }

  def extractAppConfigurationProperties(legacyData: SparkApplicationData): Map[String, String] =
    legacyData.getEnvironmentData.getSparkProperties.asScala.toMap

  def extractApplicationInfo(legacyData: SparkApplicationData): ApplicationInfoImpl = {
    val generalData = legacyData.getGeneralData
    new ApplicationInfoImpl(
      generalData.getApplicationId,
      generalData.getApplicationName,
      Seq(
        new ApplicationAttemptInfoImpl(
          Some("1"),
          new Date(generalData.getStartTime),
          new Date(generalData.getEndTime),
          generalData.getSparkUser,
          completed = true
        )
      )
    )
  }

  def extractJobDatas(legacyData: SparkApplicationData): Seq[JobDataImpl] = {
    val jobProgressData = legacyData.getJobProgressData

    def extractJobData(jobId: Int): JobDataImpl = {
      val jobInfo = jobProgressData.getJobInfo(jobId)
      new JobDataImpl(
        jobInfo.jobId,
        jobInfo.jobId.toString,
        description = None,
        submissionTime = None,
        completionTime = None,
        jobInfo.stageIds.asScala.map { _.toInt },
        Option(jobInfo.jobGroup),
        extractJobExecutionStatus(jobId),
        jobInfo.numTasks,
        jobInfo.numActiveTasks,
        jobInfo.numCompletedTasks,
        jobInfo.numSkippedTasks,
        jobInfo.numFailedTasks,
        jobInfo.numActiveStages,
        jobInfo.completedStageIndices.size(),
        jobInfo.numSkippedStages,
        jobInfo.numFailedStages
      )
    }

    def extractJobExecutionStatus(jobId: Int): JobExecutionStatus = {
      if (jobProgressData.getCompletedJobs.contains(jobId)) {
        JobExecutionStatus.SUCCEEDED
      } else if (jobProgressData.getFailedJobs.contains(jobId)) {
        JobExecutionStatus.FAILED
      } else {
        JobExecutionStatus.UNKNOWN
      }
    }

    val sortedJobIds = jobProgressData.getJobIds.asScala.toSeq.sorted
    sortedJobIds.map { jobId => extractJobData(jobId) }
  }

  def extractStageDatas(legacyData: SparkApplicationData): Seq[StageData] = {
    val jobProgressData = legacyData.getJobProgressData

    def extractStageData(stageAttemptId: SparkJobProgressData.StageAttemptId): StageDataImpl = {
      val stageInfo = jobProgressData.getStageInfo(stageAttemptId.stageId, stageAttemptId.attemptId)
      new StageDataImpl(
        extractStageStatus(stageAttemptId),
        stageAttemptId.stageId,
        stageAttemptId.attemptId,
        stageInfo.numActiveTasks,
        stageInfo.numCompleteTasks,
        stageInfo.numFailedTasks,
        stageInfo.executorRunTime,
        stageInfo.inputBytes,
        inputRecords = 0,
        stageInfo.outputBytes,
        outputRecords = 0,
        stageInfo.shuffleReadBytes,
        shuffleReadRecords = 0,
        stageInfo.shuffleWriteBytes,
        shuffleWriteRecords = 0,
        stageInfo.memoryBytesSpilled,
        stageInfo.diskBytesSpilled,
        stageInfo.name,
        stageInfo.description,
        schedulingPool = "",
        accumulatorUpdates = Seq.empty,
        tasks = None,
        executorSummary = None
      )
    }

    def extractStageStatus(stageAttemptId: SparkJobProgressData.StageAttemptId): StageStatus = {
      if (jobProgressData.getCompletedStages.contains(stageAttemptId)) {
        StageStatus.COMPLETE
      } else if (jobProgressData.getFailedStages.contains(stageAttemptId)) {
        StageStatus.FAILED
      } else {
        StageStatus.PENDING
      }
    }

    val sortedStageAttemptIds = jobProgressData.getStageAttemptIds.asScala.toSeq.sortBy { stageAttemptId =>
      (stageAttemptId.stageId, stageAttemptId.attemptId)
    }
    sortedStageAttemptIds.map { stageAttemptId => extractStageData(stageAttemptId) }
  }

  def extractExecutorSummaries(legacyData: SparkApplicationData): Seq[ExecutorSummaryImpl] = {
    val executorData = legacyData.getExecutorData

    def extractExecutorSummary(executorId: String): ExecutorSummaryImpl = {
      val executorInfo = executorData.getExecutorInfo(executorId)
      new ExecutorSummaryImpl(
        executorInfo.execId,
        executorInfo.hostPort,
        executorInfo.rddBlocks,
        executorInfo.memUsed,
        executorInfo.diskUsed,
        executorInfo.activeTasks,
        executorInfo.failedTasks,
        executorInfo.completedTasks,
        executorInfo.totalTasks,
        executorInfo.duration,
        executorInfo.inputBytes,
        executorInfo.shuffleRead,
        executorInfo.shuffleWrite,
        executorInfo.maxMem,
        executorInfo.totalGCTime,
        executorLogs = Map.empty
      )
    }

    val sortedExecutorIds = {
      val executorIds = executorData.getExecutors.asScala.toSeq
      Try(executorIds.sortBy { _.toInt }).getOrElse(executorIds.sorted)
    }
    sortedExecutorIds.map { executorId => extractExecutorSummary(executorId) }
  }

  def extractBaseModels(analyticJob: AnalyticJob, sparkApplicationData: SparkApplicationData): List[BaseModel] = {
    val sparkModels = new ListBuffer[BaseModel]

    // environment明细
    addSparkEnv(analyticJob, sparkApplicationData, sparkModels)
    // spark app明细
    addSparkApp(analyticJob, sparkApplicationData, sparkModels)
    // executor明细
    addSparkExecutors(analyticJob, sparkApplicationData, sparkModels)
    // jobs明细
    addSparkJobs(analyticJob, sparkApplicationData, sparkModels)
    // stages明细
    addSparkStages(analyticJob, sparkApplicationData, sparkModels)
    // tasks明细
    addSparkTasks(analyticJob, sparkApplicationData, sparkModels)
    // storages明细
    addSparkStorages(analyticJob, sparkApplicationData, sparkModels)
    sparkModels.toList
  }

  private def addSparkStorages(analyticJob: AnalyticJob, sparkDataCollection: SparkApplicationData, listBuffer: ListBuffer[BaseModel]): Unit = {
    sparkDataCollection.getStorageData.getStorageStatusList.asScala.foreach { storage =>
      val blkMngrId = storage.blockManagerId
      listBuffer += SparkStorage(analyticJob.getAppId,
        blkMngrId.executorId,
        blkMngrId.host,
        blkMngrId.port,
        blkMngrId.toString(),
        storage.maxMem,
        storage.cacheSize,
        storage.diskUsed,
        storage.memUsed,
        storage.memRemaining,
        storage.numBlocks,
        storage.numRddBlocks,
        analyticJob.getClusterName)
    }
  }

  private def addSparkStages(analyticJob: AnalyticJob, sparkDataCollection: SparkApplicationData, listBuffer: ListBuffer[BaseModel]): Unit = {
    val sparkJobStageData = sparkDataCollection.getJobProgressData
    sparkJobStageData.getStageIdToInfo.values.asScala.foreach { stage =>
      listBuffer += SparkStage(analyticJob.getAppId,
        stage.jobId,
        stage.stageId,
        stage.attemptId,
        stage.parentIds,
        StringEscapeUtils.escapeJava(stage.name),
        StringEscapeUtils.escapeJava(stage.description),
        stage.accumulables,
        stage.status,
        stage.failureReason,
        stage.numActiveTasks,
        stage.numKilledTasks,
        stage.numCompleteTasks,
        stage.numFailedTasks,
        stage.executorRunTime,
        stage.executorCpuTime,
        stage.duration,
        stage.inputBytes,
        stage.inputRecords,
        stage.outputBytes,
        stage.outputRecords,
        stage.shuffleReadBytes,
        stage.shuffleReadRecords,
        stage.shuffleWriteBytes,
        stage.shuffleWriteRecords,
        stage.memoryBytesSpilled,
        stage.diskBytesSpilled,
        analyticJob.getClusterName)
    }
  }


  private def addSparkTasks(analyticJob: AnalyticJob, sparkDataCollection: SparkApplicationData, listBuffer: ListBuffer[BaseModel]): Unit = {
    val sparkJobStageData = sparkDataCollection.getJobProgressData
    sparkJobStageData.getTaskIdToInfo.values.asScala.foreach { task =>
      listBuffer += SparkTask(analyticJob.getAppId,
        task.jobId,
        task.stageId,
        task.executorId,
        task.taskId,
        task.attemptId,
        new Timestamp(task.launchTime),
        new Timestamp(task.finishTime),
        task.duration,
        task.gettingResultTime,
        task.status,
        task.accumulables,
        task.host,
        task.locality,
        task.errorMessage,
        task.executorDeserTime,
        task.executorDeserCpuTime,
        task.executorRunTime,
        task.executorCpuTime,
        task.resultSize,
        task.jvmGCTime,
        task.resultSerialTime,
        task.memoryBytesSpilled,
        task.diskBytesSpilled,
        task.peakExecutionMemory,
        task.inputBytesRead,
        task.inputRecordRead,
        task.outputBytesWritten,
        task.outputRecordsWritten,
        task.shuffleRemoteBlocksFetched,
        task.shuffleLocalBlocksFetched,
        task.shuffleRemoteBytesRead,
        task.shuffleLocalBytesRead,
        task.shuffleFetchWaitTime,
        task.shuffleRecordsRead,
        task.shuffleBytesWritten,
        task.shuffleRecordsWritten,
        task.shuffleWriteTime,
        analyticJob.getClusterName)
    }
  }

  private def addSparkJobs(analyticJob: AnalyticJob, sparkDataCollection: SparkApplicationData, listBuffer: ListBuffer[BaseModel]): Unit = {
    val sparkJobStageData = sparkDataCollection.getJobProgressData
    sparkJobStageData.getJobIdToInfo.values.asScala.foreach { job =>
      listBuffer += SparkJob(analyticJob.getAppId,
        job.jobId,
        job.jobGroup,
        job.stageIds.asScala.mkString(","),
        new Timestamp(job.startTime),
        new Timestamp(job.endTime),
        job.status,
        job.error,
        job.failedStageIds,
        job.numTasks,
        job.numActiveTasks,
        job.numCompletedTasks,
        job.numSkippedTasks,
        job.numFailedTasks,
        job.numKilledTasks,
        job.getFailureRate,
        job.numActiveStages,
        job.numSkippedStages,
        job.numFailedStages,
        analyticJob.getClusterName)
    }
  }

  private def addSparkExecutors(analyticJob: AnalyticJob, sparkDataCollection: SparkApplicationData, listBuffer: ListBuffer[BaseModel]): Unit = {
    val sparkExecutorData = sparkDataCollection.getExecutorData()
    sparkExecutorData.getExecutors.asScala.foreach { executorId =>
      val executorInfo = sparkExecutorData.getExecutorInfo(executorId)
      listBuffer += SparkExecutor(analyticJob.getAppId,
        executorInfo.execId,
        executorInfo.hostPort,
        executorInfo.rddBlocks,
        executorInfo.memUsed,
        executorInfo.maxMem,
        executorInfo.diskUsed,
        executorInfo.completedTasks,
        executorInfo.failedTasks,
        executorInfo.totalTasks,
        executorInfo.duration,
        executorInfo.inputBytes,
        executorInfo.outputBytes,
        executorInfo.shuffleRead,
        executorInfo.shuffleWrite,
        executorInfo.inputRecord,
        executorInfo.outputRecord,
        executorInfo.stdout,
        executorInfo.stderr,
        new Timestamp(executorInfo.startTime),
        new Timestamp(executorInfo.finishTime),
        executorInfo.finishReason,
        analyticJob.getClusterName)
    }
  }

  private def addSparkEnv(analyticJob: AnalyticJob, sparkDataCollection: SparkApplicationData, listBuffer: ListBuffer[BaseModel]): Unit = {
    val env = sparkDataCollection.getEnvironmentData()
    listBuffer += SparkEnv(analyticJob.getAppId,
      StringEscapeUtils.escapeJava(env.getJVMInformations.toString),
      StringEscapeUtils.escapeJava(env.getSparkProperties.toString),
      StringEscapeUtils.escapeJava(env.getSystemProperties.toString),
      StringEscapeUtils.escapeJava(env.getClassPathEntries.toString),
      analyticJob.getClusterName)
  }

  private def addSparkApp(analyticJob: AnalyticJob, sparkDataCollection: SparkApplicationData, listBuffer: ListBuffer[BaseModel]) = {
    val generalData = sparkDataCollection.getGeneralData()
    val viewAcls =
      if (generalData.getViewAcls == null || generalData.getViewAcls.isEmpty) ""
      else generalData.getViewAcls.asScala.mkString(",")
    val adminAcls =
      if (generalData.getAdminAcls == null || generalData.getAdminAcls.isEmpty) ""
      else generalData.getAdminAcls.asScala.mkString(",")

    listBuffer += SparkApp(analyticJob.getAppId,
      generalData.getApplicationName,
      generalData.getAttemptId,
      analyticJob.getQueueName,
      analyticJob.getTrackingUrl,
      analyticJob.getUser,
      generalData.getSparkUser,
      analyticJob.getVcoreSeconds,
      analyticJob.getMemorySeconds,
      new Timestamp(generalData.getStartTime),
      new Timestamp(generalData.getEndTime),
      analyticJob.getFinalStatus,
      viewAcls,
      adminAcls,
      analyticJob.getClusterName)
  }
}
