package com.linkedin.drelephant.tez.data;

public class TezEntity {
    private String entity;
    private String entityType;
    private String relatedEntities;
    private String otherInfo;
    private String events;

    public String getEvents() {
        return events;
    }

    public void setEvents(String events) {
        this.events = events;
    }


    public TezEntity() {
    }

    public TezEntity(String entity, String entityType, String relatedEntities, String otherInfo, String events) {
        this.entity = entity;
        this.entityType = entityType;
        this.relatedEntities = relatedEntities;
        this.otherInfo = otherInfo;
        this.events = events;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getRelatedEntities() {
        return relatedEntities;
    }

    public void setRelatedEntities(String relatedEntities) {
        this.relatedEntities = relatedEntities;
    }

    public String getOtherInfo() {
        return otherInfo;
    }

    public void setOtherInfo(String otherInfo) {
        this.otherInfo = otherInfo;
    }
}
