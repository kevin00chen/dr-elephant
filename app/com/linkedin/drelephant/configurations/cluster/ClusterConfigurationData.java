package com.linkedin.drelephant.configurations.cluster;


import java.util.Map;

public class ClusterConfigurationData {
    private final String _clusterName;
    private final String _host;
    private final Map<String, String> _clusterParams;

    public ClusterConfigurationData(String _clusterName, String _host, Map<String, String> _clusterParams) {
        this._clusterName = _clusterName;
        this._host = _host;
        this._clusterParams = _clusterParams;
    }

    public Map<String, String> getClusterParams() {
        return _clusterParams;
    }

    public String getClusterName() {
        return _clusterName;
    }

    public String getHost() {
        return _host;
    }
}
