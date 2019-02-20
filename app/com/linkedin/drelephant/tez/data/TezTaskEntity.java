package com.linkedin.drelephant.tez.data;

public class TezTaskEntity extends TezEntity {
    private String taskId;

    private long taskStartedTime = 0L;
    private long taskFinishedTime = 0L;
    private long taskEndTime = 0L;

    private long timeTaken = 0L;

    private String status;
    private String diagnostics;
    private String counters;

    private String successfulAttemptId;
    private String firstFailedAttemptId;
    private String relatedVertexId;

    private TezTaskAttemptEntity tezTaskAttemptEntity;

    public TezTaskEntity(TezEntity tezEntity) {
        super(tezEntity.getEntity(), tezEntity.getEntityType(), tezEntity.getRelatedEntities(), tezEntity.getOtherInfo(), tezEntity.getEvents());
        this.taskId = tezEntity.getEntity();
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public long getTaskStartedTime() {
        return taskStartedTime;
    }

    public void setTaskStartedTime(long taskStartedTime) {
        this.taskStartedTime = taskStartedTime;
    }

    public long getTaskFinishedTime() {
        return taskFinishedTime;
    }

    public void setTaskFinishedTime(long taskFinishedTime) {
        this.taskFinishedTime = taskFinishedTime;
    }

    public long getTaskEndTime() {
        return taskEndTime;
    }

    public void setTaskEndTime(long taskEndTime) {
        this.taskEndTime = taskEndTime;
    }

    public long getTimeTaken() {
        return timeTaken;
    }

    public void setTimeTaken(long timeTaken) {
        this.timeTaken = timeTaken;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getCounters() {
        return counters;
    }

    public void setCounters(String counters) {
        this.counters = counters;
    }

    public String getSuccessfulAttemptId() {
        return successfulAttemptId;
    }

    public void setSuccessfulAttemptId(String successfulAttemptId) {
        this.successfulAttemptId = successfulAttemptId;
    }

    public TezTaskAttemptEntity getTezTaskAttemptEntity() {
        return tezTaskAttemptEntity;
    }

    public void setTezTaskAttemptEntity(TezTaskAttemptEntity tezTaskAttemptEntity) {
        this.tezTaskAttemptEntity = tezTaskAttemptEntity;
    }

    public String getRelatedVertexId() {
        return relatedVertexId;
    }

    public void setRelatedVertexId(String relatedVertexId) {
        this.relatedVertexId = relatedVertexId;
    }

    public String getFirstFailedAttemptId() {
        return firstFailedAttemptId;
    }

    public void setFirstFailedAttemptId(String firstFailedAttemptId) {
        this.firstFailedAttemptId = firstFailedAttemptId;
    }
}


