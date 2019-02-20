package com.linkedin.drelephant.tez.data;

public class TezContainerEntity extends TezEntity {
    private String containerId;
    private long containerLaunchedTime = 0L;
    private long containerStopedTime = 0L;
    private int exitStatus;

    public TezContainerEntity(TezEntity tezEntity) {
        super(tezEntity.getEntity(), tezEntity.getEntityType(), tezEntity.getRelatedEntities(), tezEntity.getOtherInfo(), tezEntity.getEvents());
        this.containerId = tezEntity.getEntity();
    }

    public long getContainerLaunchedTime() {
        return containerLaunchedTime;
    }

    public void setContainerLaunchedTime(long containerLaunchedTime) {
        this.containerLaunchedTime = containerLaunchedTime;
    }

    public long getContainerStopedTime() {
        return containerStopedTime;
    }

    public void setContainerStopedTime(long containerStopedTime) {
        this.containerStopedTime = containerStopedTime;
    }

    public int getExitStatus() {
        return exitStatus;
    }

    public void setExitStatus(int exitStatus) {
        this.exitStatus = exitStatus;
    }
}
