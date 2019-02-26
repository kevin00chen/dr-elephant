/*
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
 *
 */
package com.linkedin.drelephant.tez.fetchers;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.analysis.*;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.tez.data.*;
import com.linkedin.drelephant.tez.heuristics.MapperTimeHeuristic;
import com.linkedin.drelephant.util.ThreadContextMR2;
import com.linkedin.drelephant.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.codehaus.jackson.JsonNode;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Task level data mining for Tez Tasks from timeline server API
 */

public class TezHDFSFetcher implements ElephantFetcher<TezApplicationData> {

    private static final Logger logger = Logger.getLogger(TezHDFSFetcher.class);

    private static final String TEZ_HISTORY_DIR = "tez.simple.history.logging.dir";
    public static final String LOG_FILE_NAME_PREFIX = "history.txt";
    private String hdfsFilePathPrefix;

    private JSONFactory _jsonFactory;
    private Map<String, FileSystem> _clusterToFs;

    public TezHDFSFetcher(FetcherConfigurationData fetcherConfData) throws IOException {
        Map<String, Map<String, String>> clusterParams = fetcherConfData.getParamsToCluster();

        Map<String, FileSystem> clusterToFs = new HashMap<String, FileSystem>();
        Map<String, String> clusterToRM = new HashMap<String, String>();
        Configuration conf = new Configuration();

        for (String clusterName : clusterParams.keySet()) {
            Utils.setClusterConf(conf, fetcherConfData.getParamsToCluster().get(clusterName));
            try {
//                hdfsFilePathPrefix = conf.get(TEZ_HISTORY_DIR);
                hdfsFilePathPrefix = "/tmp/tez";
                URI uri = new URI("hdfs://" + conf.get("dfs.namenode.rpc-address") + hdfsFilePathPrefix);
//                URI uri = new URI("hdfs://localhost:9000" + hdfsFilePathPrefix);
                clusterToFs.put(clusterName, FileSystem.get(uri, conf));
                clusterToRM.put(clusterName, "http://" + conf.get("yarn.resourcemanager.webapp.address"));
            } catch(URISyntaxException ex) {
                clusterToFs.put(clusterName, FileSystem.get(conf));
            }
        }
        this._clusterToFs = clusterToFs;
        _jsonFactory = new JSONFactory();
    }


    public static void main(String[] args) throws Exception {
        Map<ApplicationType, ElephantFetcher> _typeToFetcher = ElephantContext.instance()._typeToFetcher;
        TezHDFSFetcher tezFetcher = (TezHDFSFetcher) _typeToFetcher.get(new ApplicationType("tez"));
        AnalyticJob analyticJob = new AnalyticJob();
        analyticJob.setAppId("application_1550656828536_11265");
        analyticJob.setClusterName("Data-Hive-ETL");
//        analyticJob.setClusterName("test-hivemetrics");
        analyticJob.setFinalStatus("SUCCEEDED");

        TezApplicationData jobData = tezFetcher.fetchData(analyticJob);

        System.out.println("===");
    }

    private Path getAppAttemptFilePath(FileSystem fs, final String appId) throws IOException {
        Path result = null;

        PathFilter filter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(appId.replace("application_", LOG_FILE_NAME_PREFIX + ".appattempt_"));
            }
        };

        FileStatus[] attemptsList = fs.listStatus(new Path(hdfsFilePathPrefix), filter);

        if (attemptsList.length == 1) {
            result = attemptsList[0].getPath();
        } else if (attemptsList.length > 1) {
            List<String> attemptPaths = new ArrayList<String>();
            for (FileStatus fileStatus : attemptsList) {
                String attemptPath = fileStatus.getPath().toUri().getPath();
                attemptPaths.add(attemptPath);
            }
            Collections.sort(attemptPaths, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o2.compareTo(o1);
                }
            });
            result = new Path(attemptPaths.get(0));
        }
        return result;
    }

    public TezApplicationData fetchData(AnalyticJob analyticJob) throws IOException, AuthenticationException {
        int maxSize = 0;
        final String appId = analyticJob.getAppId();
        String cluster = analyticJob.getClusterName();
        FileSystem fs = _clusterToFs.get(cluster);

        Path appAttemptPath = getAppAttemptFilePath(fs, appId);

        if (appAttemptPath == null) {
            logger.info("Tez HDFS File not exists: " + appId);
        }

        TezApplicationData jobData = new TezApplicationData();
        jobData.setAppId(appId);

        List<TezEntity> tezEvents = new ArrayList<TezEntity>();

        // 只分析运行成功任务的文件
        if ("SUCCEEDED".equals(analyticJob.getFinalStatus())) {
            InputStream inputStream = new BufferedInputStream(fs.open(appAttemptPath));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String line = "";
            String newLine;

            while((newLine = br.readLine()) != null){
                int length = newLine.length();
                char lastChar = newLine.charAt(length - 1);

                if (lastChar != '\u0001') {
                    line += newLine;
                    continue;
                } else {
                    if (!line.equals("")) {
                        line += newLine;
                    } else {
                        line = newLine;
                    }
                }
                tezEvents.add(_jsonFactory.parseToTezEntity(line));
                line = "";
            }
            br.close();
        }

        // 解析HDFS LOG
        TezApplicationEntity tezApplicationEntity = analyzeLogFile(appId, tezEvents);
        reloadTezEntities(tezApplicationEntity);

        jobData.setConf(tezApplicationEntity.getConf());
        jobData.setTezApplicationEntity(tezApplicationEntity);

        List<TezTaskData> mapperListAggregate = new ArrayList<TezTaskData>();
        List<TezTaskData> reducerListAggregate = new ArrayList<TezTaskData>();
        List<TezTaskData> scopeListAggregate = new ArrayList<TezTaskData>();

        String dagCountersStr = "";

        for (String dagId : tezApplicationEntity.getAppAttemptEntity().getDagEntities().keySet()) {
            TezDagEntity tezDagEntity = tezApplicationEntity.getAppAttemptEntity().getDagEntities().get(dagId);
            dagCountersStr = tezDagEntity.getCounters();

            jobData.setStartTime(tezDagEntity.getDagStartedTime());
            jobData.setFinishTime(tezDagEntity.getDagEndTime());
            String state = tezDagEntity.getStatus();

            if (state.equals("SUCCEEDED")) {
                jobData.setSucceeded(true);
            } else  {
                jobData.setSucceeded(false);
            }
            List<TezTaskData> mapperList = new ArrayList<TezTaskData>();
            List<TezTaskData> reducerList = new ArrayList<TezTaskData>();
            List<TezTaskData> scopeTaskList = new ArrayList<TezTaskData>();

            _jsonFactory.getTaskDataAll(tezApplicationEntity.getAppAttemptEntity(), dagId, mapperList, reducerList, scopeTaskList);

            if(mapperList.size() + reducerList.size() + scopeTaskList.size() > maxSize){
                mapperListAggregate = mapperList;
                reducerListAggregate = reducerList;
                scopeListAggregate = scopeTaskList;
                maxSize = mapperList.size() + reducerList.size() + scopeTaskList.size();
            }


        }

        TezTaskData[] mapperData = mapperListAggregate.toArray(new TezTaskData[mapperListAggregate.size()]);
        TezTaskData[] reducerData = reducerListAggregate.toArray(new TezTaskData[reducerListAggregate.size()]);
        TezTaskData[] scopeTaskData = scopeListAggregate.toArray(new TezTaskData[scopeListAggregate.size()]);

        TezCounterData dagCounter = _jsonFactory.getDagCounter(dagCountersStr);

        jobData.setCounters(dagCounter).setMapTaskData(mapperData).setReduceTaskData(reducerData).setScopeTasks(scopeTaskData);

        return jobData;
    }

    /**
     * 重新组装task, vertex, dag
     * @param tezApplicationEntity
     */
    private void reloadTezEntities(TezApplicationEntity tezApplicationEntity) {
        // attempt封装到task中
        for (String taskId : tezApplicationEntity.getAppAttemptEntity().getTezTaskEntityMap().keySet()) {
            TezTaskEntity tezTaskEntity = tezApplicationEntity.getAppAttemptEntity().getTezTaskEntityMap().get(taskId);
            Map<String, TezTaskAttemptEntity> taskAttemptEntities = tezApplicationEntity.getAppAttemptEntity().getTezTaskAttemptById();
            TezTaskAttemptEntity tezTaskAttemptEntity = null;

            // 如果当前task状态为成功，则绑定成功的attempt到该task
            if (tezTaskEntity.getStatus().equals("SUCCEEDED")) {
                tezTaskAttemptEntity = taskAttemptEntities.get(tezTaskEntity.getSuccessfulAttemptId());
            } else { // 否则绑定该task第一个failed的attempt，attempt中没有取到taskid，所以根据命名规则来拼接
                long firstAttemptFinishTime = Long.MAX_VALUE;
                String firstFailedAttemptId = "";
                for (int i = 0; i < 5; i ++) {
                    String tempAttemptId = taskId.replace("task_", "attempt_") + "_" + i;
                    if (taskAttemptEntities.containsKey(tempAttemptId)) {
                        TezTaskAttemptEntity tempTezTaskAttempt = taskAttemptEntities.get(tempAttemptId);
                        long finishTime = tempTezTaskAttempt.getAttemptEndTime();

                        if( finishTime < firstAttemptFinishTime) {
                            tezTaskAttemptEntity = tempTezTaskAttempt;
                            firstFailedAttemptId = tempAttemptId;
                            firstAttemptFinishTime = finishTime;
                        }
                    }
                }
                tezTaskEntity.setFirstFailedAttemptId(firstFailedAttemptId);
            }

            tezTaskEntity.setTezTaskAttemptEntity(tezTaskAttemptEntity);

            String relatedVertexId = tezTaskEntity.getRelatedVertexId();
            if (StringUtils.isEmpty(relatedVertexId)) {
                relatedVertexId = tezTaskEntity.getTaskId().substring(0, tezTaskEntity.getTaskId().lastIndexOf("_")).replace("task_", "vertex_");
            }
            tezApplicationEntity.getAppAttemptEntity().getTezVertexEntityMap().get(relatedVertexId).getTezTaskEntityMap().put(taskId, tezTaskEntity);
        }

        // vertex封装到dag中
        for (String vertexId : tezApplicationEntity.getAppAttemptEntity().getTezVertexEntityMap().keySet()) {
            TezVertexEntity tezVertexEntity = tezApplicationEntity.getAppAttemptEntity().getTezVertexEntityMap().get(vertexId);
            String relatedDag = tezVertexEntity.getRelatedDagId();
            tezApplicationEntity.getAppAttemptEntity().getDagEntities().get(relatedDag).getTezVertexEntityMap().put(vertexId, tezVertexEntity);
        }
    }

    /**
     * 解析TEZ log文件内容
     * @param appId
     * @param tezEvents
     * @return
     */
    private TezApplicationEntity analyzeLogFile(String appId, List<TezEntity> tezEvents) throws IOException, AuthenticationException {
        TezApplicationEntity tezApplicationEntity = null;
        /**
         * 主要有七种事件类型：(从上到下的包含关系，全部记录解析后得到一个TezApplicationEntity对象)
         * TEZ_APPLICATION
         * TEZ_APPLICATION_ATTEMPT
         * TEZ_DAG_ID
         * TEZ_VERTEX_ID
         * TEZ_TASK_ID
         * TEZ_TASK_ATTEMPT_ID
         * TEZ_CONTAINER_ID (略)
         */
        for (TezEntity tezEvent : tezEvents) {
            String entity = tezEvent.getEntity();
            String entityType = tezEvent.getEntityType();
            String otherInfo = tezEvent.getOtherInfo();
            String events = tezEvent.getEvents();
            String relatedEntities = tezEvent.getRelatedEntities();

            if (entity.startsWith(appId.replace("application_", "tez_application_")) && entityType.equals("TEZ_APPLICATION")) {
                tezApplicationEntity = new TezApplicationEntity(tezEvent);

                Properties jobConf;
                jobConf = _jsonFactory.getProperties(otherInfo);
                tezApplicationEntity.setConf(jobConf);
            } else if (entity.startsWith(appId.replace("application_", "tez_appattempt_")) && entityType.equals("TEZ_APPLICATION_ATTEMPT")) {
                TezAppAttemptEntity tezAppAttemptEntity = tezApplicationEntity.getAppAttemptEntity() == null ? new TezAppAttemptEntity(tezEvent) : tezApplicationEntity.getAppAttemptEntity();

                for (Map<String, String> event : extractInfoFronEvents(events)) {
                    String eventType = event.get("eventtype");
                    long timestamp = Long.parseLong(event.get("ts"));
                    if ("AM_LAUNCHED".equals(eventType)) {
                        tezAppAttemptEntity.setAmLaunchedTime(timestamp);
                    } else if ("AM_STARTED".equals(eventType)) {
                        tezAppAttemptEntity.setAmStartedTime(timestamp);
                    }
                }

                if (!StringUtils.isEmpty(otherInfo)) {
                    JsonNode otherInfoNode = ThreadContextMR2.readJsonNodeFromStr(otherInfo);
                    tezAppAttemptEntity.setAppSubmitTime(otherInfoNode.path("appSubmitTime").getLongValue());
                }

                tezApplicationEntity.setAppAttemptEntity(tezAppAttemptEntity);
                System.out.println();

            } else if (entity.startsWith(appId.replace("application_", "dag_")) && entityType.equals("TEZ_DAG_ID")) {
                Map<String, TezDagEntity> dagEntities = tezApplicationEntity.getAppAttemptEntity().getDagEntities();

                TezDagEntity dagEntity = dagEntities.getOrDefault(entity, new TezDagEntity(tezEvent));

                if (!StringUtils.isEmpty(events)) {
                    for (Map<String, String> event : extractInfoFronEvents(events)) {
                        String eventType = event.get("eventtype");
                        long timestamp = Long.parseLong(event.get("ts"));

                        if ("DAG_INITIALIZED".equals(eventType)) {
                            dagEntity.setDagInitializedTime(timestamp);
                        } else if ("DAG_STARTED".equals(eventType)) {
                            dagEntity.setDagStartedTime(timestamp);
                        } else if ("DAG_FINISHED".equals(eventType)) {
                            dagEntity.setDagFinishedTime(timestamp);
                        } else if ("DAG_SUBMITTED".equals(eventType)) {
                            dagEntity.setDagSubmittedTime(timestamp);
                        }

                    }
                }

                if (!StringUtils.isEmpty(otherInfo)) {
                    JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(otherInfo);
                    if (rootNode.has("status")) {
                        dagEntity.setStatus(rootNode.path("status").getValueAsText());
                        dagEntity.setDagEndTime(rootNode.path("endTime").getLongValue());
                        dagEntity.setTimeTaken(rootNode.path("timeTaken").getLongValue());
                        dagEntity.setDiagnostics(rootNode.path("diagnostics").getValueAsText());
                        dagEntity.setNumFailedTaskAttempts(rootNode.path("numFailedTaskAttempts").getIntValue());
                        dagEntity.setNumKilledTaskAttempts(rootNode.path("numKilledTaskAttempts").getIntValue());
                        dagEntity.setNumSucceededTasks(rootNode.path("numCompletedTasks").getIntValue());
                        dagEntity.setNumCompletedTasks(rootNode.path("numSucceededTasks").getIntValue());
                        dagEntity.setNumKilledTasks(rootNode.path("numKilledTasks").getIntValue());
                        dagEntity.setNumFailedTasks(rootNode.path("numFailedTasks").getIntValue());
                        dagEntity.setCounters(rootNode.path("counters").toString());
                    } else if (rootNode.has("dagPlan")) {
                        dagEntity.setDagName(rootNode.path("dagPlan").path("dagName").getTextValue());
                    }
                }

                dagEntities.put(dagEntity.getDagId(), dagEntity);
            } else if (entity.startsWith(appId.replace("application_", "vertex_")) && entityType.equals("TEZ_VERTEX_ID")) {
                Map<String, TezVertexEntity> vertexEntities = tezApplicationEntity.getAppAttemptEntity().getTezVertexEntityMap();

                TezVertexEntity vertexEntity = vertexEntities.getOrDefault(entity, new TezVertexEntity(tezEvent));

                if (!StringUtils.isEmpty(relatedEntities)) {
                    String relatedDagId = getRelatedIdByName(relatedEntities, "TEZ_DAG_ID");
                    vertexEntity.setRelatedDagId(relatedDagId);
                }

                if (!StringUtils.isEmpty(events)) {
                    for (Map<String, String> event : extractInfoFronEvents(events)) {
                        String eventType = event.get("eventtype");
                        long timestamp = Long.parseLong(event.get("ts"));

                        if (timestamp > 0) {
                            if ("VERTEX_INITIALIZED".equals(eventType)) {
                                vertexEntity.setVertexInitializedTime(timestamp);
                            } else if ("VERTEX_STARTED".equals(eventType)) {
                                vertexEntity.setVertexStartedTime(timestamp);
                            } else if ("VERTEX_FINISHED".equals(eventType)) {
                                vertexEntity.setVertexFinishedTime(timestamp);
                            }
                        }
                    }
                }
                if (!StringUtils.isEmpty(otherInfo)) {
                    JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(otherInfo);
                    if (rootNode.has("vertexName")) {
                        vertexEntity.setName(rootNode.path("vertexName").getTextValue());
                        vertexEntity.setProcessorClassName(rootNode.path("processorClassName").getTextValue());
                        vertexEntity.setNumTasks(rootNode.path("numTasks").getIntValue());
                    }
                    if (rootNode.has("endTime")) {
                        vertexEntity.setVertexEndTime(rootNode.path("endTime").getLongValue());
                        vertexEntity.setStatus(rootNode.path("status").getTextValue());
                        vertexEntity.setDiagnostics(rootNode.path("diagnostics").getTextValue());
                        vertexEntity.setCounters(rootNode.path("counters").toString());
                        vertexEntity.setStats(rootNode.path("stat").toString());
                        vertexEntity.setNumFailedTaskAttempts(rootNode.path("numFailedTaskAttempts").getValueAsLong());
                        vertexEntity.setNumKilledTaskAttempts(rootNode.path("numKilledTaskAttempts").getValueAsLong());
                        vertexEntity.setNumCompletedTasks(rootNode.path("numCompletedTasks").getValueAsLong());
                        vertexEntity.setNumSucceededTasks(rootNode.path("numSucceededTasks").getValueAsLong());
                        vertexEntity.setNumKilledTasks(rootNode.path("numKilledTasks").getValueAsLong());
                        vertexEntity.setNumFailedTasks(rootNode.path("numFailedTasks").getValueAsLong());
                    }
                }

                vertexEntities.put(vertexEntity.getVertexId(), vertexEntity);
            } else if (entity.startsWith(appId.replace("application_", "task_")) && entityType.equals("TEZ_TASK_ID")) {
                Map<String, TezTaskEntity> taskEntities = tezApplicationEntity.getAppAttemptEntity().getTezTaskEntityMap();

                TezTaskEntity taskEntity = taskEntities.getOrDefault(entity, new TezTaskEntity(tezEvent));

                if (!StringUtils.isEmpty(relatedEntities)) {
                    String relatedVertexId = getRelatedIdByName(relatedEntities, "TEZ_VERTEX_ID");
                    taskEntity.setRelatedVertexId(relatedVertexId);
                }

                if (!StringUtils.isEmpty(events)) {
                    for (Map<String, String> event : extractInfoFronEvents(events)) {
                        String eventType = event.get("eventtype");
                        long timestamp = Long.parseLong(event.get("ts"));

                        if (timestamp > 0) {
                            if ("TASK_STARTED".equals(eventType)) {
                                taskEntity.setTaskStartedTime(timestamp);
                            } else if ("TASK_FINISHED".equals(eventType)) {
                                taskEntity.setTaskFinishedTime(timestamp);
                            }
                        }
                    }
                }
                if (!StringUtils.isEmpty(otherInfo)) {
                    JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(otherInfo);
                    if (rootNode.has("endTime")) {
                        taskEntity.setTaskEndTime(rootNode.path("endTime").getLongValue());
                        taskEntity.setStatus(rootNode.path("status").getTextValue());
                        taskEntity.setDiagnostics(rootNode.path("diagnostics").getTextValue());
                        taskEntity.setCounters(rootNode.path("counters").toString());
                        taskEntity.setTimeTaken(rootNode.path("timeTaken").getLongValue());
                        taskEntity.setSuccessfulAttemptId(rootNode.path("successfulAttemptId").getTextValue());
                    }
                }

                taskEntities.put(taskEntity.getTaskId(), taskEntity);

                // 测试failed task
//                if (entity.equals("task_1546926861976_0061_1_01_000000")) {
//                    taskEntity.setStatus("FAILED");
//                }
            } else if (entity.startsWith(appId.replace("application_", "attempt_")) && entityType.equals("TEZ_TASK_ATTEMPT_ID")) {
                Map<String, TezTaskAttemptEntity> attemptEntities = tezApplicationEntity.getAppAttemptEntity().getTezTaskAttemptById();
                TezTaskAttemptEntity attemptEntity = attemptEntities.getOrDefault(entity, new TezTaskAttemptEntity(tezEvent));

                if (!StringUtils.isEmpty(relatedEntities)) {
                    String relatedNodeId = getRelatedIdByName(relatedEntities, "nodeId");
                    attemptEntity.setHost(relatedNodeId.split(":")[0]);
                }

                if (!StringUtils.isEmpty(events)) {
                    for (Map<String, String> event : extractInfoFronEvents(events)) {
                        String eventType = event.get("eventtype");
                        long timestamp = Long.parseLong(event.get("ts"));

                        if (timestamp > 0) {
                            if ("TASK_ATTEMPT_STARTED".equals(eventType)) {
                                attemptEntity.setAttemptStartedTime(timestamp);
                            } else if ("TASK_ATTEMPT_FINISHED".equals(eventType)) {
                                attemptEntity.setAttemptFinishedTime(timestamp);
                            }
                        }
                    }
                }

                JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(otherInfo);
                if (rootNode.has("endTime")) {
                    attemptEntity.setCreationTime(rootNode.path("creationTime").getValueAsLong());
                    attemptEntity.setAllocationTime(rootNode.path("allocationTime").getValueAsLong());
                    attemptEntity.setAttemptEndTime(rootNode.path("endTime").getLongValue());
                    attemptEntity.setStatus(rootNode.path("status").getTextValue());
                    attemptEntity.setDiagnostics(rootNode.path("diagnostics").getTextValue());
                    attemptEntity.setCounters(rootNode.path("counters").toString());
                    attemptEntity.setTimeTaken(rootNode.path("timeTaken").getLongValue());
                }

                attemptEntities.put(attemptEntity.getAttemptId(), attemptEntity);

                // 测试failed task
//                if (entity.equals("attempt_1546926861976_0061_1_01_000000_0")) {
//                    String tempAttId = "attempt_1546926861976_0061_1_01_000000_1";
//
//                    // attepmt 0置为失败，完成时间延后1秒
//                    attemptEntity.setStatus("FAILED");
//
//                    TezTaskAttemptEntity tempAtt = (TezTaskAttemptEntity) attemptEntity.clone();
//
//                    attemptEntity.setAttemptEndTime(attemptEntity.getAttemptEndTime() + 1000);
//
//                    attemptEntities.put(tempAttId, tempAtt);
//                }
            }
            // 暂时不处理Container
            else if (entity.startsWith(appId.replace("application_", "tez_container_")) && entityType.equals("TEZ_CONTAINER_ID")) {
                // DO NOTHING
            }
        }

        return tezApplicationEntity;
    }

    private String getRelatedIdByName(String events, String relatedId) throws IOException, AuthenticationException {
        String result = "";
        JsonNode eventsNode = ThreadContextMR2.readJsonNodeFromStr(events);
        for (int i = 0; i < eventsNode.size(); i ++) {
            JsonNode tempNode = eventsNode.get(i);
            if (tempNode.path("entitytype").getTextValue().equals(relatedId)) {
                result = tempNode.path("entity").getTextValue();
            }
        }
        return result;
    }

    private List<Map<String, String>> extractInfoFronEvents(String events) throws IOException, AuthenticationException {
        JsonNode eventsNode = ThreadContextMR2.readJsonNodeFromStr(events);

        List<Map<String, String>> result = new ArrayList<Map<String, String>>();

        for (int i = 0; i < eventsNode.size(); i ++) {
            Map<String, String> tempMap = new HashMap<String, String>();

            String timestamp = String.valueOf(eventsNode.get(i).path("ts").getLongValue());
            String eventType = eventsNode.get(i).path("eventtype").getTextValue();
            tempMap.put("ts", timestamp);
            tempMap.put("eventtype", eventType);
            result.add(tempMap);
        }
        return result;
    }

    /**
     * JSONFactory class provides functionality to parse mined job data from timeline server.
     */

    private class JSONFactory {
        private TezEntity parseToTezEntity(String line) throws IOException, AuthenticationException {
            JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(line);
            String entity = rootNode.path("entity").getTextValue();
            String entytytype = rootNode.path("entitytype").getTextValue();
            String otherinfo = rootNode.path("otherinfo").toString();
            String relatedEntities = rootNode.path("relatedEntities").toString();
            String event = rootNode.path("events").toString();
            return new TezEntity(entity, entytytype, relatedEntities, otherinfo, event);
        }

        private Properties getProperties(String str) throws IOException, AuthenticationException {
            Properties jobConf = new Properties();
            JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(str);
            JsonNode configs = rootNode.path("config");
            Iterator<String> keys = configs.getFieldNames();
            String key = "";
            String value = "";
            while (keys.hasNext()) {
                key = keys.next();
                value = configs.get(key).getTextValue();
                jobConf.put(key, value);
            }
            return jobConf;
        }

        private TezCounterData getDagCounter(String str) throws IOException, AuthenticationException {
            TezCounterData holder = new TezCounterData();
            JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(str);
            JsonNode groups = rootNode.path("counterGroups");

            for (JsonNode group : groups) {
                for (JsonNode counter : group.path("counters")) {
                    String name = counter.get("counterName").getTextValue();
                    String groupName = group.get("counterGroupName").getTextValue();
                    Long value = counter.get("counterValue").getLongValue();
                    holder.set(groupName, name, value);
                }
            }

            return holder;
        }

        private void getTaskDataAll(TezAppAttemptEntity tezAppAttemptEntity, String dagId, List<TezTaskData> mapperList,
                                    List<TezTaskData> reducerList, List<TezTaskData> scopeTaskList) throws IOException, AuthenticationException {
            boolean isMapVertex = false;
            Map<String, TezVertexEntity> tezVertexEntityMap = tezAppAttemptEntity.getDagEntities().get(dagId).getTezVertexEntityMap();

            for (String vertexId : tezVertexEntityMap.keySet()) {
                TezVertexEntity vertex = tezVertexEntityMap.get(vertexId);
                String vertexClass = vertex.getProcessorClassName();

                if (vertexClass != null) {
                    if (vertexClass.equals("org.apache.hadoop.hive.ql.exec.tez.MapTezProcessor")) {
                        isMapVertex = true;
                        getTaskDataByVertexId(vertex.getTezTaskEntityMap(), mapperList, isMapVertex);
                    }
                    else if (vertexClass.equals("org.apache.hadoop.hive.ql.exec.tez.ReduceTezProcessor")) {
                        isMapVertex = false;
                        getTaskDataByVertexId(vertex.getTezTaskEntityMap(), reducerList, isMapVertex);
                    }
                    else if (vertexClass.equals("org.apache.pig.backend.hadoop.executionengine.tez.runtime.PigProcessor")) {
                        isMapVertex = false;
                        getTaskDataByVertexId(vertex.getTezTaskEntityMap(), scopeTaskList, isMapVertex);
                    }
                }
            }
        }

        private void getTaskDataByVertexId(Map<String, TezTaskEntity> tezTaskEntityMap, List<TezTaskData> taskList,
                                           boolean isMapTask) throws IOException, AuthenticationException {

            for (String taskId : tezTaskEntityMap.keySet()) {
                TezTaskEntity tezTaskEntity = tezTaskEntityMap.get(taskId);
                String state = tezTaskEntity.getStatus();
                String attemptId = tezTaskEntity.getSuccessfulAttemptId();
                if (!"SUCCEEDED".equals(state)) {
                    attemptId = tezTaskEntity.getFirstFailedAttemptId();
                }

                taskList.add(new TezTaskData(taskId, attemptId));
            }

            getTaskData(taskList, isMapTask, tezTaskEntityMap);
        }

        private void getTaskData(List<TezTaskData> taskList, boolean isMapTask, Map<String, TezTaskEntity> tezTaskEntityMap) throws IOException, AuthenticationException {

            for(int i = 0; i < taskList.size(); i++) {
                TezTaskData data = taskList.get(i);
                String taskId = data.getTaskId();
                if (tezTaskEntityMap.containsKey(taskId)) {
                    TezTaskEntity tezTaskEntity = tezTaskEntityMap.get(taskId);
                    String countersStr = tezTaskEntity.getCounters();

                    TezCounterData taskCounter = getTaskCounter(countersStr);

                    TezTaskAttemptEntity tezTaskAttemptEntity = tezTaskEntity.getTezTaskAttemptEntity();
                    if (tezTaskAttemptEntity == null) {
                        taskList.remove(i);
                    } else {
                        long[] taskExecTime = getTaskAttemptExecTime(tezTaskAttemptEntity, isMapTask);
                        data.setCounter(taskCounter);
                        data.setTime(taskExecTime);
                    }

                }
            }
        }

        private TezCounterData getTaskCounter(String str) throws IOException, AuthenticationException {
            JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(str);
            JsonNode groups = rootNode.path("counterGroups");
            TezCounterData holder = new TezCounterData();

            //Fetch task level metrics
            for (JsonNode group : groups) {
                for (JsonNode counter : group.path("counters")) {
                    String name = counter.get("counterName").getTextValue();
                    String groupName = group.get("counterGroupName").getTextValue();
                    Long value = counter.get("counterValue").getLongValue();
                    holder.set(groupName, name, value);
                }
            }

            return holder;
        }

        private long[] getTaskAttemptExecTime(TezTaskAttemptEntity tezTaskAttemptEntity, boolean isMapTask) throws IOException, AuthenticationException {
            long startTime = tezTaskAttemptEntity.getAttemptStartedTime();
            long finishTime = tezTaskAttemptEntity.getAttemptEndTime();

            if (startTime <= 0) {
                startTime = tezTaskAttemptEntity.getCreationTime();
            }

            long shuffleTime = 0;
            long mergeTime = 0;

            JsonNode rootNode = ThreadContextMR2.readJsonNodeFromStr(tezTaskAttemptEntity.getCounters());
            JsonNode groups = rootNode.path("counterGroups");

            for (JsonNode group : groups) {
                for (JsonNode counter : group.path("counters")) {
                    String name = counter.get("counterName").getTextValue();
                    if (!isMapTask && name.equals("MERGE_PHASE_TIME")) {
                        mergeTime = counter.get("counterValue").getLongValue();
                    }
                    else if (!isMapTask && name.equals("SHUFFLE_PHASE_TIME")){
                        shuffleTime = counter.get("counterValue").getLongValue();
                    }

                }
            }

            long[] time = new long[] { finishTime - startTime, shuffleTime, mergeTime, startTime, finishTime };

            return time;
        }

    }
}