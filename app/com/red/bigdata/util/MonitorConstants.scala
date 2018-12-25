package com.red.bigdata.util

/**
  * Created by chenkaiming on 2018/12/25.
  */
object MonitorConstants {
  val CONFIG_MASTER="spark.master"
  val CONFIG_APPNAME="spark.app.name"
  val CONFIG_CHECKPOINT_DIR="job.checkpoint_dir"
  val CONFIG_REMEMBER_INTERVAL="job.remember_interval"
  val CONFIG_STREAMING_MAXRATE="spark.streaming.receiver.maxRate"
  val CONFIG_STREAMING_BACKPRESSURE="spark.streaming.backpressure.enabled"
  val CONFIG_YARN_MAX_EXECUTOR_FAILURES="spark.yarn.max.executor.failures"
  val CONFIG_EXECTOR_MEMORY="spark.executor.memory"
  val CONFIG_SPECULATION="spark.speculation"
  val CONFIG_NUM_PROCESSOR="processor.threads_num"
  val CONFIG_PROCESSOR_SLEEP_MILLS="processor.sleeptime"

  val BATCH_INSERT_SIZE = 100
}
