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
import com.linkedin.drelephant.util.Utils;
import org.apache.commons.io.FileUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class GenericTezDataSkewHeuristic extends GenericDataSkewHeuristic {
    private String taskType;

    public GenericTezDataSkewHeuristic(List<TezCounterData.CounterName> counterNames, HeuristicConfigurationData heuristicConfData, String taskType) {
        super(counterNames, heuristicConfData);
        this.taskType = taskType;
    }

    @Override
    public HeuristicResult apply(TezApplicationData data) {
        if (!data.getSucceeded()) {
            return null;
        }

        // 按全部Task进行分析
        TezTaskData[] tasks = getTasks(data);
        HeuristicResult result = getResultByTasks(tasks, "全部Task", "全部Task");

        Severity maxSeverity = Severity.NONE;

        Map<String, TezTaskData[]> vtxTasksMap = null;
        if (taskType.equalsIgnoreCase("MAP")) {
            vtxTasksMap = data.getVtxMapTasks();
        } else if (taskType.equalsIgnoreCase("REDUCE")) {
            vtxTasksMap = data.getVtxReduceTasks();
        }

        for (String vtxId : vtxTasksMap.keySet()) {
            TezTaskData[] vtxTasks = vtxTasksMap.get(vtxId);
            if (vtxTasks != null) {
                HeuristicResult tmpResult = getResultByTasks(vtxTasks, "Vertex名称", vtxId);
                maxSeverity = Severity.max(maxSeverity, tmpResult.getSeverity());
                for (HeuristicResultDetails tmp : tmpResult.getHeuristicResultDetails()) {
                    result.addResultDetail(tmp.getName(), tmp.getValue(), tmp.getDetails());
                }
            } else {
//                System.out.println();
            }

        }
        result.setSeverity(maxSeverity);
        return result;
    }

    public HeuristicResult getResultByTasks(TezTaskData[] tasks, String resultKey, String vertexName) {
        //Gathering data for checking time skew
        List<Long> timeTaken = new ArrayList<Long>();

        for(int i = 0; i < tasks.length; i++) {
            if (tasks[i].isSampled()) {
                timeTaken.add(tasks[i].getTotalRunTimeMs());
            }
        }

        long[][] groupsTime = Statistics.findTwoGroups(Longs.toArray(timeTaken));

        long timeAvg1 = Statistics.average(groupsTime[0]);
        long timeAvg2 = Statistics.average(groupsTime[1]);

        //seconds are used for calculating deviation as they provide a better idea than millisecond.
        long timeAvgSec1 = TimeUnit.MILLISECONDS.toSeconds(timeAvg1);
        long timeAvgSec2 = TimeUnit.MILLISECONDS.toSeconds(timeAvg2);

        long minTime = Math.min(timeAvgSec1, timeAvgSec2);
        long diffTime = Math.abs(timeAvgSec1 - timeAvgSec2);

        //using the same deviation limits for time skew as for data skew. It can be changed in the fututre.
        Severity severityTime = getDeviationSeverity(minTime, diffTime);

        //This reduces severity if number of tasks is insignificant
        severityTime = Severity.min(severityTime,
                Severity.getSeverityAscending(groupsTime[0].length, numTasksLimits[0], numTasksLimits[1], numTasksLimits[2],
                        numTasksLimits[3]));

        //Gather data
        List<Long> inputSizes = new ArrayList<Long>();

        for (int i = 0; i < tasks.length; i++) {
            if (tasks[i].isSampled()) {

                long inputByte = 0;
                for (TezCounterData.CounterName counterName : _counterNames) {
                    inputByte += tasks[i].getCounters().get(counterName);
                }

                inputSizes.add(inputByte);
            }
        }

        long[][] groups = Statistics.findTwoGroups(Longs.toArray(inputSizes));

        long avg1 = Statistics.average(groups[0]);
        long avg2 = Statistics.average(groups[1]);

        long min = Math.min(avg1, avg2);
        long diff = Math.abs(avg2 - avg1);

        Severity severityData = getDeviationSeverity(min, diff);

        //This reduces severity if the largest file sizes are insignificant
        severityData = Severity.min(severityData, getFilesSeverity(avg2));

        //This reduces severity if number of tasks is insignificant
        severityData = Severity.min(severityData,
                Severity.getSeverityAscending(groups[0].length, numTasksLimits[0], numTasksLimits[1], numTasksLimits[2],
                        numTasksLimits[3]));

        Severity severity = Severity.max(severityData, severityTime);

        HeuristicResult result =
                new HeuristicResult(_heuristicConfData.getClassName(), _heuristicConfData.getHeuristicName(), severity,
                        Utils.getHeuristicScore(severityData, tasks.length));

        result.addResultDetail(resultKey, vertexName);
        result.addResultDetail("Data skew (Number of tasks)", Integer.toString(tasks.length));
        result.addResultDetail("Data skew (Group A)",
                groups[0].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg1) + " avg");
        result.addResultDetail("Data skew (Group B)",
                groups[1].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg2) + " avg");

        result.addResultDetail("Time skew (Number of tasks)", Integer.toString(tasks.length));
        result.addResultDetail("Time skew (Group A)",
                groupsTime[0].length + " tasks @ " + convertTimeMs(timeAvg1) + " avg");
        result.addResultDetail("Time skew (Group B)",
                groupsTime[1].length + " tasks @ " + convertTimeMs(timeAvg2) + " avg");

        return result;
    }
}
