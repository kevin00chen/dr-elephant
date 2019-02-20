package com.linkedin.drelephant.tez.data;

import java.util.HashMap;
import java.util.Map;

public class TezDagEntity extends TezEntity {
    private String dagId;

    private String dagName;
    private long dagInitializedTime = 0L;
    private long dagStartedTime = 0L;
    private long dagFinishedTime = 0L;
    private long dagSubmittedTime = 0L;

    private String status;
    private long dagEndTime = 0L;
    private long timeTaken = 0L;
    private String diagnostics;
    private int numFailedTaskAttempts;
    private int numKilledTaskAttempts;
    private int numCompletedTasks;
    private int numSucceededTasks;
    private int numKilledTasks;
    private int numFailedTasks;

    // TODO counters暂未解析
    private String counters;

    Map<String, TezVertexEntity> tezVertexEntityMap;

    public TezDagEntity(TezEntity tezEntity) {
        super(tezEntity.getEntity(), tezEntity.getEntityType(), tezEntity.getRelatedEntities(), tezEntity.getOtherInfo(), tezEntity.getEvents());
        this.dagId = tezEntity.getEntity();
        this.tezVertexEntityMap = new HashMap<String, TezVertexEntity>();
    }

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getDagInitializedTime() {
        return dagInitializedTime;
    }

    public void setDagInitializedTime(long dagInitializedTime) {
        this.dagInitializedTime = dagInitializedTime;
    }

    public long getDagStartedTime() {
        return dagStartedTime;
    }

    public void setDagStartedTime(long dagStartedTime) {
        this.dagStartedTime = dagStartedTime;
    }

    public long getDagFinishedTime() {
        return dagFinishedTime;
    }

    public void setDagFinishedTime(long dagFinishedTime) {
        this.dagFinishedTime = dagFinishedTime;
    }

    public Map<String, TezVertexEntity> getTezVertexEntityMap() {
        return tezVertexEntityMap;
    }

    public long getDagSubmittedTime() {
        return dagSubmittedTime;
    }

    public void setDagSubmittedTime(long dagSubmittedTime) {
        this.dagSubmittedTime = dagSubmittedTime;
    }

    public long getDagEndTime() {
        return dagEndTime;
    }

    public void setDagEndTime(long dagEndTime) {
        this.dagEndTime = dagEndTime;
    }

    public long getTimeTaken() {
        return timeTaken;
    }

    public void setTimeTaken(long timeTaken) {
        this.timeTaken = timeTaken;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public int getNumFailedTaskAttempts() {
        return numFailedTaskAttempts;
    }

    public void setNumFailedTaskAttempts(int numFailedTaskAttempts) {
        this.numFailedTaskAttempts = numFailedTaskAttempts;
    }

    public int getNumKilledTaskAttempts() {
        return numKilledTaskAttempts;
    }

    public void setNumKilledTaskAttempts(int numKilledTaskAttempts) {
        this.numKilledTaskAttempts = numKilledTaskAttempts;
    }

    public int getNumCompletedTasks() {
        return numCompletedTasks;
    }

    public void setNumCompletedTasks(int numCompletedTasks) {
        this.numCompletedTasks = numCompletedTasks;
    }

    public int getNumSucceededTasks() {
        return numSucceededTasks;
    }

    public void setNumSucceededTasks(int numSucceededTasks) {
        this.numSucceededTasks = numSucceededTasks;
    }

    public int getNumKilledTasks() {
        return numKilledTasks;
    }

    public void setNumKilledTasks(int numKilledTasks) {
        this.numKilledTasks = numKilledTasks;
    }

    public int getNumFailedTasks() {
        return numFailedTasks;
    }

    public void setNumFailedTasks(int numFailedTasks) {
        this.numFailedTasks = numFailedTasks;
    }

    public String getCounters() {
        return counters;
    }

    public void setCounters(String counters) {
        this.counters = counters;
    }

    public String getDagName() {
        return dagName;
    }

    public void setDagName(String dagName) {
        this.dagName = dagName;
    }
}
