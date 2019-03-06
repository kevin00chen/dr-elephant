/*
 * Copyright 2017 Electronic Arts Inc.
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
package com.linkedin.drelephant.tez.heuristics;


import com.google.common.primitives.Longs;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.HeuristicResultDetails;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.tez.data.TezApplicationData;
import com.linkedin.drelephant.tez.data.TezCounterData;
import com.linkedin.drelephant.tez.data.TezTaskData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.drelephant.util.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


/**
 * This Heuristic analyses the skewness in the task input data
 */
public class MapperDataSkewHeuristic extends GenericTezDataSkewHeuristic {
  private static final Logger logger = Logger.getLogger(MapperDataSkewHeuristic.class);

  @Override
  protected TezTaskData[] getTasks(TezApplicationData data) {
    return data.getMapTaskData();
  }

  public MapperDataSkewHeuristic(HeuristicConfigurationData heuristicConfData) {
    super(Arrays.asList(
            TezCounterData.CounterName.HDFS_BYTES_READ,
            TezCounterData.CounterName.S3A_BYTES_READ,
            TezCounterData.CounterName.S3N_BYTES_READ,
            TezCounterData.CounterName.S3_BYTES_READ
    ),
            heuristicConfData, "map");
  }

}