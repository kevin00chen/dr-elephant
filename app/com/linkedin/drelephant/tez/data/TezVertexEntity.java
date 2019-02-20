package com.linkedin.drelephant.tez.data;

import java.util.HashMap;
import java.util.Map;

public class TezVertexEntity extends TezEntity {
    private String vertexId;
    private String name;
    private String processorClassName;

    private int numTasks;
    private long vertexInitializedTime = 0L;
    private long vertexStartedTime = 0L;
    private long vertexFinishedTime = 0L;
    private long vertexEndTime = 0L;
    private String status;

    private long numFailedTaskAttempts = 0L;
    private long numKilledTaskAttempts = 0L;
    private long numCompletedTasks = 0L;
    private long numSucceededTasks = 0L;
    private long numKilledTasks = 0L;
    private long numFailedTasks = 0L;

    private String counters;
    private String diagnostics;

    private String relatedDagId;

    private Map<String, TezTaskEntity> tezTaskEntityMap;

    //TODO 未解析
    /*
    "stats":{
        "firstTaskStartTime":1548672811844,
        "firstTasksToStart":[
            "task_1546926861976_0061_1_00_000000"
        ],
        "lastTaskFinishTime":1548672921091,
        "lastTasksToFinish":[
            "task_1546926861976_0061_1_00_000022"
        ],
        "minTaskDuration":69107,
        "maxTaskDuration":89971,
        "avgTaskDuration":79455.23076923077,
        "shortestDurationTasks":[
            "task_1546926861976_0061_1_00_000023"
        ],
        "longestDurationTasks":[
            "task_1546926861976_0061_1_00_000007"
        ]
    },
     */
    private String stats;

    public TezVertexEntity(TezEntity tezEntity) {
        super(tezEntity.getEntity(), tezEntity.getEntityType(), tezEntity.getRelatedEntities(), tezEntity.getOtherInfo(), tezEntity.getEvents());
        this.vertexId = tezEntity.getEntity();
        this.tezTaskEntityMap = new HashMap<String, TezTaskEntity>();
    }

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public void setNumTasks(int numTasks) {
        this.numTasks = numTasks;
    }

    public long getVertexInitializedTime() {
        return vertexInitializedTime;
    }

    public void setVertexInitializedTime(long vertexInitializedTime) {
        this.vertexInitializedTime = vertexInitializedTime;
    }

    public long getVertexStartedTime() {
        return vertexStartedTime;
    }

    public void setVertexStartedTime(long vertexStartedTime) {
        this.vertexStartedTime = vertexStartedTime;
    }

    public long getVertexFinishedTime() {
        return vertexFinishedTime;
    }

    public void setVertexFinishedTime(long vertexFinishedTime) {
        this.vertexFinishedTime = vertexFinishedTime;
    }

    public long getVertexEndTime() {
        return vertexEndTime;
    }

    public void setVertexEndTime(long vertexEndTime) {
        this.vertexEndTime = vertexEndTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getNumFailedTaskAttempts() {
        return numFailedTaskAttempts;
    }

    public void setNumFailedTaskAttempts(long numFailedTaskAttempts) {
        this.numFailedTaskAttempts = numFailedTaskAttempts;
    }

    public long getNumKilledTaskAttempts() {
        return numKilledTaskAttempts;
    }

    public void setNumKilledTaskAttempts(long numKilledTaskAttempts) {
        this.numKilledTaskAttempts = numKilledTaskAttempts;
    }

    public long getNumCompletedTasks() {
        return numCompletedTasks;
    }

    public void setNumCompletedTasks(long numCompletedTasks) {
        this.numCompletedTasks = numCompletedTasks;
    }

    public long getNumSucceededTasks() {
        return numSucceededTasks;
    }

    public void setNumSucceededTasks(long numSucceededTasks) {
        this.numSucceededTasks = numSucceededTasks;
    }

    public long getNumKilledTasks() {
        return numKilledTasks;
    }

    public void setNumKilledTasks(long numKilledTasks) {
        this.numKilledTasks = numKilledTasks;
    }

    public long getNumFailedTasks() {
        return numFailedTasks;
    }

    public void setNumFailedTasks(long numFailedTasks) {
        this.numFailedTasks = numFailedTasks;
    }

    public String getStats() {
        return stats;
    }

    public void setStats(String stats) {
        this.stats = stats;
    }

    public String getProcessorClassName() {
        return processorClassName;
    }

    public void setProcessorClassName(String processorClassName) {
        this.processorClassName = processorClassName;
    }

    public String getCounters() {
        return counters;
    }

    public void setCounters(String counters) {
        this.counters = counters;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getRelatedDagId() {
        return relatedDagId;
    }

    public void setRelatedDagId(String relatedDagId) {
        this.relatedDagId = relatedDagId;
    }

    public Map<String, TezTaskEntity> getTezTaskEntityMap() {
        return tezTaskEntityMap;
    }
}
