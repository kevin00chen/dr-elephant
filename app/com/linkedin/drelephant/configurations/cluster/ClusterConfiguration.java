package com.linkedin.drelephant.configurations.cluster;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData;
import com.linkedin.drelephant.util.Utils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClusterConfiguration {
    private static final Logger logger = Logger.getLogger(ClusterConfiguration.class);

    public List<ClusterConfigurationData> getClustersConfDataList() {
        return _clustersConfDataList;
    }

    public void setClustersConfDataList(List<ClusterConfigurationData> _clustersConfDataList) {
        this._clustersConfDataList = _clustersConfDataList;
    }

    private List<ClusterConfigurationData> _clustersConfDataList;

    public ClusterConfiguration(Element configuration) {
        parseClusterConfiguration(configuration);
    }

    private void parseClusterConfiguration(Element configuration) {
        _clustersConfDataList = new ArrayList<ClusterConfigurationData>();

        NodeList nodes = configuration.getChildNodes();
        int n = 0;
        for (int i = 0; i < nodes.getLength(); i++) {
            // Each heuristic node
            Node node = nodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                n++;
                Element clusterElem = (Element) node;

                String clusterName;
                Node clusterNameNode = clusterElem.getElementsByTagName("name").item(0);
                if (clusterNameNode == null) {
                    throw new RuntimeException("No tag 'name' in cluster " + n);
                }
                clusterName = clusterNameNode.getTextContent();
                if (clusterName.equals("")) {
                    throw new RuntimeException("Empty tag 'name' in cluster " + n);
                }

                // Check if parameters are defined for the cluster
                Map<String, String> paramsMap = Utils.getConfigurationParameters(clusterElem);

                ClusterConfigurationData clusterData = new ClusterConfigurationData(clusterName, paramsMap);
                _clustersConfDataList.add(clusterData);
            }
        }
    }

    public static void main(String[] args) {
        Document document = Utils.loadXMLDoc("/Users/chenkaiming/files/workspace/apache/dr-elephant/app-conf/ClusterConf.xml");
        ClusterConfiguration c = new ClusterConfiguration(document.getDocumentElement());
        System.out.println();

    }
}
