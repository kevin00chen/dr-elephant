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

package org.apache.spark.deploy.history

import java.io.InputStream
import java.util
import java.util.{Set => JSet, Properties, List => JList, HashSet => JHashSet, ArrayList => JArrayList}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics

import scala.collection.mutable
import scala.collection.mutable.HashMap

import com.linkedin.drelephant.analysis.ApplicationType
import com.linkedin.drelephant.spark.legacydata._
import com.linkedin.drelephant.spark.legacydata.SparkExecutorData.ExecutorInfo
import com.linkedin.drelephant.spark.legacydata.SparkJobProgressData.JobInfo

import org.apache.spark.{ExceptionFailure, Resubmitted, SparkContext, SparkConf}
import org.apache.spark.scheduler._
import org.apache.spark.storage.{RDDInfo, StorageStatus, StorageStatusListener, StorageStatusTrackingListener}
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.storage.StorageListener
import org.apache.spark.util.collection.OpenHashSet

/**
 * This class wraps the logic of collecting the data in SparkEventListeners into the
 * HadoopApplicationData instances.
 *
 * Notice:
 * This has to live in Spark's scope because ApplicationEventListener is in private[spark] scope. And it is problematic
 * to compile if written in Java.
 */
class SparkDataCollection extends SparkApplicationData {
  import SparkDataCollection._

  val sparkConf = new SparkConf
  lazy val applicationEventListener = new ApplicationEventListener()
  lazy val jobProgressListener = new JobProgressListener(sparkConf)
  lazy val environmentListener = new EnvironmentListener()
  lazy val storageStatusListener = new StorageStatusListener(sparkConf)
  lazy val executorsListener = new ExecutorsListener(storageStatusListener)
  lazy val storageListener = new StorageListener(storageStatusListener)

  // This is a customized listener that tracks peak used memory
  // The original listener only tracks the current in use memory which is useless in offline scenario.
  lazy val storageStatusTrackingListener = new StorageStatusTrackingListener()

  private var _applicationData: SparkGeneralData = null;
  private var _jobProgressData: SparkJobProgressData = null;
  private var _environmentData: SparkEnvironmentData = null;
  private var _executorData: SparkExecutorData = null;
  private var _storageData: SparkStorageData = null;
  private var _isThrottled: Boolean = false;

  def throttle(): Unit = {
    _isThrottled = true
  }

  override def isThrottled(): Boolean = _isThrottled

  override def getApplicationType(): ApplicationType = APPLICATION_TYPE

  override def getConf(): Properties = getEnvironmentData().getSparkProperties()

  override def isEmpty(): Boolean = !isThrottled() && getExecutorData().getExecutors.isEmpty()

  override def getGeneralData(): SparkGeneralData = {
    if (_applicationData == null) {
      _applicationData = new SparkGeneralData()

      applicationEventListener.adminAcls match {
        case Some(s: String) => {
          _applicationData.setAdminAcls(stringToSet(s))
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.viewAcls match {
        case Some(s: String) => {
          _applicationData.setViewAcls(stringToSet(s))
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.appId match {
        case Some(s: String) => {
          _applicationData.setApplicationId(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.appName match {
        case Some(s: String) => {
          _applicationData.setApplicationName(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.sparkUser match {
        case Some(s: String) => {
          _applicationData.setSparkUser(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.startTime match {
        case Some(s: Long) => {
          _applicationData.setStartTime(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.endTime match {
        case Some(s: Long) => {
          _applicationData.setEndTime(s)
        }
        case None => {
          // do nothing
        }
      }

      applicationEventListener.appAttemptId match {
        case Some(s: String) => {
          _applicationData.setAttemptId(s)
        }
        case None => {
          // do nothing
        }
      }
    }
    _applicationData
  }

  override def getEnvironmentData(): SparkEnvironmentData = {
    if (_environmentData == null) {
      // Notice: we ignore jvmInformation and classpathEntries, because they are less likely to be used by any analyzer.
      _environmentData = new SparkEnvironmentData()
      environmentListener.systemProperties.foreach { case (name, value) =>
        _environmentData.addSystemProperty(name, value)
      }
      environmentListener.sparkProperties.foreach { case (name, value) =>
        _environmentData.addSparkProperty(name, value)
      }
      environmentListener.jvmInformation.foreach { case (name, value) =>
        _environmentData.addJVMProperty(name, value)
      }
      environmentListener.classpathEntries.foreach { case (name, value) =>
        _environmentData.addClassPathProperty(name, value)
      }
    }
    _environmentData
  }

  override def getExecutorData(): SparkExecutorData = {
    if (_executorData == null) {
      _executorData = new SparkExecutorData()

      for (statusId <- 0 until executorsListener.storageStatusList.size) {
        val info = new ExecutorInfo()

        val status = executorsListener.storageStatusList(statusId)

        info.execId = status.blockManagerId.executorId
        info.hostPort = status.blockManagerId.hostPort
        info.rddBlocks = status.numBlocks

        // Use a customized listener to fetch the peak memory used, the data contained in status are
        // the current used memory that is not useful in offline settings.
        info.memUsed = storageStatusTrackingListener.executorIdToMaxUsedMem.getOrElse(info.execId, 0L)
        info.maxMem = status.maxMem
        info.diskUsed = status.diskUsed
        info.activeTasks = executorsListener.executorToTasksActive.getOrElse(info.execId, 0)
        info.failedTasks = executorsListener.executorToTasksFailed.getOrElse(info.execId, 0)
        info.completedTasks = executorsListener.executorToTasksComplete.getOrElse(info.execId, 0)
        info.totalTasks = info.activeTasks + info.failedTasks + info.completedTasks
        info.duration = executorsListener.executorToDuration.getOrElse(info.execId, 0L)
        info.totalGCTime = executorsListener.executorToGCDuration.getOrElse(info.execId, 0L)
        info.inputBytes = executorsListener.executorToInputBytes.getOrElse(info.execId, 0L)
        info.shuffleRead = executorsListener.executorToShuffleRead.getOrElse(info.execId, 0L)
        info.shuffleWrite = executorsListener.executorToShuffleWrite.getOrElse(info.execId, 0L)

        info.inputRecord = executorsListener.executorToInputRecords.getOrElse(info.execId, 0L)
        info.outputRecord = executorsListener.executorToOutputRecords.getOrElse(info.execId, 0L)

        val logUrlsMap = executorsListener.executorToLogUrls.getOrElse(info.execId, Map.empty)
        info.stdout = logUrlsMap.getOrElse("stdout", "")
        info.stderr = logUrlsMap.getOrElse("stderr", "")

        val executorUIData = executorsListener.executorIdToData.getOrElse(info.execId, new ExecutorUIData(0))
        info.startTime = executorUIData.startTime
        info.finishTime = info.startTime + info.duration
        info.finishReason = executorUIData.finishReason.getOrElse("")

        _executorData.setExecutorInfo(info.execId, info)
      }
    }
    _executorData
  }

  override def getJobProgressData(): SparkJobProgressData = {
    if (_jobProgressData == null) {
      _jobProgressData = new SparkJobProgressData()
      val stageIdToJobIdMap = new util.HashMap[Integer, Integer]
      val jobIdToFailedStages = new util.HashMap[Integer, (JHashSet[Integer], JList[String])]

      jobProgressListener.jobIdToData.foreach { case (id, data) =>
        data.stageIds.foreach{ case (id: Int) =>
          stageIdToJobIdMap.put(id, data.jobId)
        }
      }

      // Add Stage Info
      jobProgressListener.stageIdToData.foreach { case (id, data) =>
        val stageInfo = new SparkJobProgressData.StageInfo()
        val sparkStageInfo = jobProgressListener.stageIdToInfo.get(id._1)
        stageInfo.name = sparkStageInfo match {
          case Some(info: StageInfo) => {
            info.name
          }
          case None => {
            ""
          }
        }
        val stageId = id._1

        val jobId = if (stageIdToJobIdMap.containsKey(stageId)) {
          stageIdToJobIdMap.get(stageId).toInt
        } else {
          -1
        }

        stageInfo.jobId = jobId
        stageInfo.stageId = stageId
        stageInfo.attemptId = id._2
        stageInfo.numKilledTasks = data.numKilledTasks
        stageInfo.executorCpuTime = data.executorCpuTime
        stageInfo.inputRecords = data.inputRecords
        stageInfo.outputRecords = data.outputRecords
        stageInfo.shuffleReadRecords = data.shuffleReadRecords
        stageInfo.shuffleWriteRecords = data.shuffleWriteRecords
        stageInfo.accumulables = data.accumulables.toString
        stageInfo.description = data.description.getOrElse("")
        val stageIdInfo = jobProgressListener.stageIdToInfo.get(stageId)
        stageIdInfo match {
          case Some(info) => {
            val status = info.getStatusString

            stageInfo.status = status
            val failureReason = info.failureReason match {
              case Some(reason) => reason
              case _ => ""
            }
            stageInfo.failureReason = failureReason
            // 如果当前stage状态为failed
            if (status == "failed") {
              // 如果当前stage所属job中已有失败stage,则追加,否则新增当前stage的信息
              try {
                jobIdToFailedStages.get(stageId)._1.add(stageId)
                jobIdToFailedStages.get(stageId)._2.add(s"Stage-${stageId}\n${failureReason}")
              } catch {
                case e: Exception => {
                  val failedStageIds = new JHashSet[Integer]()
                  val failedStageReasons = new JArrayList[String]()
                  failedStageIds.add(stageId)
                  failedStageReasons.add(s"Stage-${stageId}\n${failureReason}\n")
                  jobIdToFailedStages.put(jobId, (failedStageIds, failedStageReasons))
                }
              }
            }
          }
          case _ =>
          // do nothing
        }

        stageInfo.diskBytesSpilled = data.diskBytesSpilled
        stageInfo.executorRunTime = data.executorRunTime
        sparkStageInfo match {
          case Some(info: StageInfo) => {
            val submissionTime = info.submissionTime.getOrElse(0L)
            stageInfo.duration = info.completionTime.getOrElse(submissionTime) - submissionTime
            val parentIds = info.parentIds
            stageInfo.parentIds =
              if (parentIds == null || parentIds.isEmpty) ""
              else parentIds.mkString(",")
          }
          case _ => stageInfo.duration = 0L
        }
        stageInfo.inputBytes = data.inputBytes
        stageInfo.memoryBytesSpilled = data.memoryBytesSpilled
        stageInfo.numActiveTasks = data.numActiveTasks
        stageInfo.numCompleteTasks = data.numCompleteTasks
        stageInfo.numFailedTasks = data.numFailedTasks
        stageInfo.outputBytes = data.outputBytes
        stageInfo.shuffleReadBytes = data.shuffleReadTotalBytes
        stageInfo.shuffleWriteBytes = data.shuffleWriteBytes
        addIntSetToJSet(data.completedIndices, stageInfo.completedIndices)

        data.taskData.foreach { task =>
          val taskInfo = new SparkJobProgressData.TaskInfo()
          val taskUIDataInfo = task._2.taskInfo
          val taskUIMetrics = task._2.metrics
          val errorMessage = task._2.errorMessage

          val taskId = task._1
          val taskAttemptId = task._2.taskInfo.attemptNumber

          taskInfo.stageId = id._1
          taskInfo.taskId = taskId
          taskInfo.attemptId = taskAttemptId

          taskInfo.launchTime = taskUIDataInfo.launchTime
          taskInfo.finishTime = taskUIDataInfo.finishTime
          taskInfo.gettingResultTime = taskUIDataInfo.gettingResultTime
          taskInfo.status = taskUIDataInfo.status
          taskInfo.accumulables = taskUIDataInfo.accumulables.mkString(",")
          taskInfo.host = taskUIDataInfo.host
          taskInfo.locality = taskUIDataInfo.taskLocality.toString
          taskInfo.errorMessage = taskUIDataInfo.status
          if (taskUIDataInfo.status == "SUCCESS") {
            taskInfo.duration = taskUIDataInfo.duration
          }
          taskInfo.executorId = taskUIDataInfo.executorId

          errorMessage match {
            case Some(value) => {
              taskInfo.errorMessage = value
            }
            case None => {
              // do nothing
            }
          }

          taskUIMetrics match {
            case Some(value) => {
              taskInfo.executorDeserTime = value.executorDeserializeTime
              taskInfo.executorDeserCpuTime = value.executorDeserializeCpuTime
              taskInfo.executorRunTime = value.executorRunTime
              taskInfo.executorCpuTime = value.executorCpuTime
              taskInfo.resultSize = value.resultSize
              taskInfo.jvmGCTime = value.jvmGCTime
              taskInfo.resultSerialTime = value.resultSerializationTime
              taskInfo.memoryBytesSpilled = value.memoryBytesSpilled
              taskInfo.diskBytesSpilled = value.diskBytesSpilled
              taskInfo.peakExecutionMemory = value.peakExecutionMemory

              val taskInputMetrics = value.inputMetrics
              taskInfo.inputBytesRead = taskInputMetrics.bytesRead
              taskInfo.inputRecordRead = taskInputMetrics.recordsRead

              val taskOutputMetrics = value.outputMetrics
              taskInfo.outputBytesWritten = taskOutputMetrics.bytesWritten
              taskInfo.outputRecordsWritten = taskOutputMetrics.recordsWritten

              val taskShuffleReadMetrics = value.shuffleReadMetrics
              taskInfo.shuffleRemoteBlocksFetched = taskShuffleReadMetrics.remoteBlocksFetched
              taskInfo.shuffleLocalBlocksFetched = taskShuffleReadMetrics.localBlocksFetched
              taskInfo.shuffleRemoteBytesRead = taskShuffleReadMetrics.remoteBytesRead
              taskInfo.shuffleLocalBytesRead = taskShuffleReadMetrics.localBytesRead
              taskInfo.shuffleFetchWaitTime = taskShuffleReadMetrics.fetchWaitTime
              taskInfo.shuffleRecordsRead = taskShuffleReadMetrics.recordsRead

              val taskShuffleWriteMetrics = value.shuffleWriteMetrics
              taskInfo.shuffleBytesWritten = taskShuffleWriteMetrics.bytesWritten
              taskInfo.shuffleRecordsWritten = taskShuffleWriteMetrics.recordsWritten
              taskInfo.shuffleWriteTime = taskShuffleWriteMetrics.writeTime
            }
            case None => {
              // do nothing
            }
          }

          _jobProgressData.addTaskInfo(taskId, taskAttemptId, taskInfo)
        }

        _jobProgressData.addStageInfo(id._1, id._2, stageInfo)
      }

      import collection.JavaConverters._

      // Add JobInfo
      jobProgressListener.jobIdToData.foreach { case (id, data) =>
        val jobInfo = new JobInfo()
        var failedStageIds = ""
        var error = ""

        val jobId = data.jobId
        jobInfo.jobId = jobId

        if (jobIdToFailedStages.containsKey(jobId)) {
          failedStageIds = jobIdToFailedStages.get(jobId)._1.asScala.mkString(",")
          error = jobIdToFailedStages.get(jobId)._2.asScala.mkString("\n\n")
        }

        jobInfo.failedStageIds = failedStageIds
        jobInfo.error = error
        jobInfo.jobGroup = data.jobGroup.getOrElse("")
        jobInfo.numActiveStages = data.numActiveStages
        jobInfo.numActiveTasks = data.numActiveTasks
        jobInfo.numCompletedTasks = data.numCompletedTasks
        jobInfo.numFailedStages = data.numFailedStages
        jobInfo.numFailedTasks = data.numFailedTasks
        jobInfo.numSkippedStages = data.numSkippedStages
        jobInfo.numSkippedTasks = data.numSkippedTasks
        jobInfo.numKilledTasks = data.numKilledTasks
        jobInfo.numTasks = data.numTasks
        jobInfo.status = data.status.name()

        jobInfo.startTime = data.submissionTime.getOrElse(0)
        jobInfo.endTime = data.completionTime.getOrElse(0)

        data.stageIds.foreach{ case (id: Int) =>
          jobInfo.addStageId(id)
          stageIdToJobIdMap.put(id, data.jobId)
        }

        addIntSetToJSet(data.completedStageIndices, jobInfo.completedStageIndices)

        _jobProgressData.addJobInfo(id, jobInfo)
      }

      // Add completed jobs
      jobProgressListener.completedJobs.foreach { case (data) => _jobProgressData.addCompletedJob(data.jobId) }
      // Add failed jobs
      jobProgressListener.failedJobs.foreach { case (data) => _jobProgressData.addFailedJob(data.jobId) }
      // Add completed stages
      jobProgressListener.completedStages.foreach { case (data) =>
        _jobProgressData.addCompletedStages(data.stageId, data.attemptId)
      }
      // Add failed stages
      jobProgressListener.failedStages.foreach { case (data) =>
        _jobProgressData.addFailedStages(data.stageId, data.attemptId)
      }
    }
    _jobProgressData
  }

  // This method returns a combined information from StorageStatusListener and StorageListener
  override def getStorageData(): SparkStorageData = {
    if (_storageData == null) {
      _storageData = new SparkStorageData()
      _storageData.setRddInfoList(toJList[RDDInfo](storageListener.rddInfoList))
      _storageData.setStorageStatusList(toJList[StorageStatus](storageStatusListener.storageStatusList))
    }
    _storageData
  }

  override def getAppId: String = {
    getGeneralData().getApplicationId
  }

  def load(in: InputStream, sourceName: String): Unit = {
    val replayBus = new ReplayListenerBus()
    replayBus.addListener(applicationEventListener)
    replayBus.addListener(jobProgressListener)
    replayBus.addListener(environmentListener)
    replayBus.addListener(storageStatusListener)
    replayBus.addListener(executorsListener)
    replayBus.addListener(storageListener)
    replayBus.addListener(storageStatusTrackingListener)
    replayBus.replay(in, sourceName, maybeTruncated = false)
  }
}

object SparkDataCollection {
  private val APPLICATION_TYPE = new ApplicationType("SPARK")

  def stringToSet(str: String): JSet[String] = {
    val set = new JHashSet[String]()
    str.split(",").foreach { case t: String => set.add(t)}
    set
  }

  def toJList[T](seq: Seq[T]): JList[T] = {
    val list = new JArrayList[T]()
    seq.foreach { case (item: T) => list.add(item)}
    list
  }

  def addIntSetToJSet(set: OpenHashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }

  def addIntSetToJSet(set: mutable.HashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }
}

@DeveloperApi
class ExecutorsListener(storageStatusListener: StorageStatusListener) extends SparkListener {
  val executorToTasksActive = HashMap[String, Int]()
  val executorToTasksComplete = HashMap[String, Int]()
  val executorToTasksFailed = HashMap[String, Int]()
  val executorToDuration = HashMap[String, Long]()
  val executorToGCDuration = HashMap[String, Long]()
  val executorToInputBytes = HashMap[String, Long]()
  val executorToInputRecords = HashMap[String, Long]()
  val executorToOutputBytes = HashMap[String, Long]()
  val executorToOutputRecords = HashMap[String, Long]()
  val executorToShuffleRead = HashMap[String, Long]()
  val executorToShuffleWrite = HashMap[String, Long]()
  val executorToLogUrls = HashMap[String, Map[String, String]]()
  val executorIdToData = HashMap[String, ExecutorUIData]()

  def storageStatusList: Seq[StorageStatus] = storageStatusListener.storageStatusList

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    executorToLogUrls(eid) = executorAdded.executorInfo.logUrlMap
    executorIdToData(eid) = ExecutorUIData(executorAdded.time)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    val eid = executorRemoved.executorId
    val uiData = executorIdToData(eid)
    uiData.finishTime = Some(executorRemoved.time)
    uiData.finishReason = Some(executorRemoved.reason)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    applicationStart.driverLogs.foreach { logs =>
      val storageStatus = storageStatusList.find { s =>
        s.blockManagerId.executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER ||
          s.blockManagerId.executorId == SparkContext.DRIVER_IDENTIFIER
      }
      storageStatus.foreach { s => executorToLogUrls(s.blockManagerId.executorId) = logs.toMap }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 0) + 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = info.executorId
      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          return
        case e: ExceptionFailure =>
          executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
        case _ =>
          executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
      }

      executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 1) - 1
      executorToDuration(eid) = executorToDuration.getOrElse(eid, 0L) + info.duration

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        executorToGCDuration(eid) = executorToGCDuration.getOrElse(eid, 0L) + metrics.jvmGCTime

        executorToInputBytes(eid) =
          executorToInputBytes.getOrElse(eid, 0L) + metrics.inputMetrics.bytesRead
        executorToInputRecords(eid) =
          executorToInputRecords.getOrElse(eid, 0L) + metrics.inputMetrics.recordsRead

        executorToOutputBytes(eid) =
          executorToOutputBytes.getOrElse(eid, 0L) + metrics.inputMetrics.bytesRead
        executorToOutputRecords(eid) =
          executorToOutputRecords.getOrElse(eid, 0L) + metrics.inputMetrics.recordsRead

        executorToShuffleRead(eid) =
          executorToShuffleRead.getOrElse(eid, 0L) + metrics.shuffleReadMetrics.remoteBytesRead

        executorToShuffleWrite(eid) =
          executorToShuffleWrite.getOrElse(eid, 0L) + metrics.shuffleWriteMetrics.shuffleBytesWritten
      }
    }
  }

}

/**
  * These are kept mutable and reused throughout a task's lifetime to avoid excessive reallocation.
  */
case class TaskUIData(
                       var taskInfo: TaskInfo,
                       var taskMetrics: Option[TaskMetrics] = None,
                       var errorMessage: Option[String] = None)

case class ExecutorUIData(
                           val startTime: Long,
                           var finishTime: Option[Long] = None,
                           var finishReason: Option[String] = None)