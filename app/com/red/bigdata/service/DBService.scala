package com.red.bigdata.service

/**
  * Created by chenkaiming on 2018/12/21.
  */

import com.red.bigdata.db.DatabaseAccess
import models.{AppHeuristicResult, AppHeuristicResultDetails, AppResult}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenkaiming on 2017/6/29.
  */
class DBService(dao: DatabaseAccess) {
  val logger = LoggerFactory.getLogger(this.getClass)

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
    import scala.collection.JavaConversions._
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
      case e: Exception => logger.error("Error thrown when saving MR AppResult : " + ExceptionUtils.getStackTrace(e))
    }
  }
}