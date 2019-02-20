package com.linkedin.drelephant.tez.data;

public class TezTaskAttemptEntity extends TezEntity {
    private String attemptId;
    private long attemptStartedTime = 0L;
    private long attemptFinishedTime = 0L;
    private long creationTime = 0L;
    private long allocationTime = 0L;
    private long startTime = 0L;
    private long attemptEndTime = 0L;
    private long timeTaken = 0L;
    private String status;
    private String diagnostics;
    private String counters;
    private String host;

    public TezTaskAttemptEntity() {
        super();
    }

    public TezTaskAttemptEntity(TezEntity tezEntity) {
        super(tezEntity.getEntity(), tezEntity.getEntityType(), tezEntity.getRelatedEntities(), tezEntity.getOtherInfo(), tezEntity.getEvents());
        this.attemptId = tezEntity.getEntity();
    }

    public String getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(String attemptId) {
        this.attemptId = attemptId;
    }

    public long getAttemptStartedTime() {
        return attemptStartedTime;
    }

    public void setAttemptStartedTime(long attemptStartedTime) {
        this.attemptStartedTime = attemptStartedTime;
    }

    public long getAttemptFinishedTime() {
        return attemptFinishedTime;
    }

    public void setAttemptFinishedTime(long attemptFinishedTime) {
        this.attemptFinishedTime = attemptFinishedTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getAllocationTime() {
        return allocationTime;
    }

    public void setAllocationTime(long allocationTime) {
        this.allocationTime = allocationTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getAttemptEndTime() {
        return attemptEndTime;
    }

    public void setAttemptEndTime(long attemptEndTime) {
        this.attemptEndTime = attemptEndTime;
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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
