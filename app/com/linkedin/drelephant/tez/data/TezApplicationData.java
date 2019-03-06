/*
 *
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
package com.linkedin.drelephant.tez.data;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.analysis.HadoopApplicationData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Tez Application level data structure which hold all task data
 */
public class TezApplicationData implements HadoopApplicationData {

  private static final ApplicationType APPLICATION_TYPE = new ApplicationType("TEZ");

  private String _appId = "";
  private Properties _conf;
  private boolean _succeeded = true;
  private TezTaskData[] _reduceTasks;
  private TezTaskData[] _mapTasks;
  private Map<String, TezTaskData[]> _vtxMapTasks;
  private Map<String, TezTaskData[]> _vtxReduceTasks;
  private TezTaskData[] _scopeTasks;
  private TezCounterData _counterHolder;
  private TezApplicationEntity _tezApplicationEntity;

  private long _submitTime = 0;
  private long _startTime = 0;
  private long _finishTime = 0;

  public boolean getSucceeded() {
    return _succeeded;
  }

  @Override
  public String getAppId() {
    return _appId;
  }

  @Override
  public Properties getConf() {
    return _conf;
  }

  @Override
  public ApplicationType getApplicationType() {
    return APPLICATION_TYPE;
  }

  @Override
  public boolean isEmpty() {
    return _succeeded && getMapTaskData().length == 0 && getReduceTaskData().length == 0;
  }

  public TezTaskData[] getReduceTaskData() {
    return _reduceTasks;
  }

  public TezTaskData[] getMapTaskData() {
    return _mapTasks;
  }

  public long getSubmitTime() {
    return _submitTime;
  }

  public long getStartTime() {
    return _startTime;
  }

  public long getFinishTime() {
    return _finishTime;
  }

  public TezCounterData getCounters() {
    return _counterHolder;
  }

  public TezApplicationData setCounters(TezCounterData counterHolder) {
    this._counterHolder = counterHolder;
    return this;
  }

  public TezApplicationData setAppId(String appId) {
    this._appId = appId;
    return this;
  }

  public TezApplicationData setConf(Properties conf) {
    this._conf = conf;
    return this;
  }

  public TezApplicationData setSucceeded(boolean succeeded) {
    this._succeeded = succeeded;
    return this;
  }

  public TezApplicationData setReduceTaskData(TezTaskData[] reduceTasks) {
    this._reduceTasks = reduceTasks;
    return this;
  }

  public TezApplicationData setMapTaskData(TezTaskData[] mapTasks) {
    this._mapTasks = mapTasks;
    return this;
  }

  public TezTaskData[] getScopeTasks() {
    return _scopeTasks;
  }

  public void setScopeTasks(TezTaskData[] _scopeTasks) {
    this._scopeTasks = _scopeTasks;
  }

  public TezApplicationData setSubmitTime(long submitTime) {
    this._submitTime = submitTime;
    return this;
  }

  public TezApplicationData setStartTime(long startTime) {
    this._startTime = startTime;
    return this;
  }

  public Map<String, TezTaskData[]> getVtxMapTasks() {
    return _vtxMapTasks;
  }

  public TezApplicationData setVtxMapTasks(Map<String, List<TezTaskData>> _vtxMapTasks) {
    this._vtxMapTasks = new HashMap<String, TezTaskData[]>();
    for (String vtxId : _vtxMapTasks.keySet()) {
      TezTaskData[] mapperData = _vtxMapTasks.get(vtxId).toArray(new TezTaskData[_vtxMapTasks.get(vtxId).size()]);
      this._vtxMapTasks.put(vtxId, mapperData);
    }
    return this;
  }

  public Map<String, TezTaskData[]> getVtxReduceTasks() {
    return _vtxReduceTasks;
  }

  public TezApplicationData setVtxReduceTasks(Map<String, List<TezTaskData>> _vtxReduceTasks) {
    this._vtxReduceTasks = new HashMap<String, TezTaskData[]>();
    for (String vtxId : _vtxReduceTasks.keySet()) {
      TezTaskData[] mapperData = _vtxReduceTasks.get(vtxId).toArray(new TezTaskData[_vtxReduceTasks.get(vtxId).size()]);
      this._vtxReduceTasks.put(vtxId, mapperData);
    }
    return this;
  }

  public TezApplicationData setFinishTime(long finishTime) {
    this._finishTime = finishTime;
    return this;
  }

  public TezApplicationEntity getTezApplicationEntity() {
    return _tezApplicationEntity;
  }

  public void setTezApplicationEntity(TezApplicationEntity _tezApplicationEntity) {
    this._tezApplicationEntity = _tezApplicationEntity;
  }

  public String toString(){
    return APPLICATION_TYPE.toString() + " " + _appId;
  }

}