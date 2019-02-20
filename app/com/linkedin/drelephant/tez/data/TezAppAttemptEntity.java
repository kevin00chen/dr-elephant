package com.linkedin.drelephant.tez.data;

import java.util.*;

public class TezAppAttemptEntity extends TezEntity {
    private long appSubmitTime = 0l;
    private long amLaunchedTime = 0l;
    private long amStartedTime = 0l;

    private Map<String, TezDagEntity> dagEntities;
    private Map<String, TezVertexEntity> tezVertexEntityMap;
    private Map<String, TezTaskEntity> tezTaskEntityMap;
    private Map<String, TezTaskAttemptEntity> tezTaskAttemptById;

    public TezAppAttemptEntity(TezEntity tezEntity) {
        super(tezEntity.getEntity(), tezEntity.getEntityType(), tezEntity.getRelatedEntities(), tezEntity.getOtherInfo(), tezEntity.getEvents());
        this.dagEntities = new HashMap<String, TezDagEntity>();
        this.tezVertexEntityMap = new HashMap<String, TezVertexEntity>();
        this.tezTaskEntityMap = new HashMap<String, TezTaskEntity>();
        this.tezTaskAttemptById = new HashMap<String, TezTaskAttemptEntity>();
    }

    public long getAppSubmitTime() {
        return appSubmitTime;
    }

    public void setAppSubmitTime(long appSubmitTime) {
        this.appSubmitTime = appSubmitTime;
    }

    public long getAmLaunchedTime() {
        return amLaunchedTime;
    }

    public void setAmLaunchedTime(long amLaunchedTime) {
        this.amLaunchedTime = amLaunchedTime;
    }

    public long getAmStartedTime() {
        return amStartedTime;
    }

    public void setAmStartedTime(long amStartedTime) {
        this.amStartedTime = amStartedTime;
    }

    public Map<String, TezDagEntity> getDagEntities() {
        return dagEntities;
    }

    public Map<String, TezVertexEntity> getTezVertexEntityMap() {
        return tezVertexEntityMap;
    }

    public Map<String, TezTaskEntity> getTezTaskEntityMap() {
        return tezTaskEntityMap;
    }

    public Map<String, TezTaskAttemptEntity> getTezTaskAttemptById() {
        return tezTaskAttemptById;
    }
}
