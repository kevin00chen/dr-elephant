package com.linkedin.drelephant.tez.data;

import java.util.Properties;

public class TezApplicationEntity extends TezEntity {
    private TezAppAttemptEntity appAttemptEntity;
    private String tezAppId;
    private Properties conf;

    public TezApplicationEntity(TezEntity tezEntity) {
        super(tezEntity.getEntity(), tezEntity.getEntityType(), tezEntity.getRelatedEntities(), tezEntity.getOtherInfo(), tezEntity.getEvents());
        this.tezAppId = tezEntity.getEntity();
    }

    public TezAppAttemptEntity getAppAttemptEntity() {
        return appAttemptEntity;
    }

    public void setAppAttemptEntity(TezAppAttemptEntity appAttemptEntity) {
        this.appAttemptEntity = appAttemptEntity;
    }

    public String getTezAppId() {
        return tezAppId;
    }

    public Properties getConf() {
        return conf;
    }

    public void setConf(Properties conf) {
        this.conf = conf;
    }
}
