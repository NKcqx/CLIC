package basic;

import basic.operators.Operator;
import channel.Channel;
import org.jgrapht.Graph;
import org.jgrapht.graph.AsSubgraph;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * sub-plan，保存Logical/Physical Plan的片段，用于Assign到不同平台。
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/20 3:27 下午
 */
public class Stage implements Serializable {
    private AsSubgraph<Operator, Channel> graph = null;
    private Graph<Operator, Channel> baseGraph = null;
    private String id;
    private String name;
    private String platform;
    private Set<Operator> vertexSet;
    private Set<Channel> edgeSet;

    public Stage(String id, String name, String platform, Graph<Operator, Channel> baseGraph) {
        this.id = id;
        this.name = name;
        this.platform = platform;
        this.baseGraph = baseGraph;
        this.vertexSet = new HashSet<>();
        this.edgeSet = new HashSet<>();
    }

    private void checkAndGenerateGraphView() {
        if (this.graph == null){
            graph = new AsSubgraph<>(baseGraph, vertexSet, edgeSet);
        }
    }

    public AsSubgraph<Operator, Channel> getGraph(){
        checkAndGenerateGraphView();
        return graph;
    }

    public boolean addVertex(Operator vertex){
        return this.vertexSet.add(vertex);
    }

    public boolean addEdges(Set<Channel> channels){
        return this.edgeSet.addAll(channels);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Operator> getTails() {
        checkAndGenerateGraphView();
        return this.graph.vertexSet().stream()
                .filter(operator -> graph.outDegreeOf(operator) == 0)
                .collect(Collectors.toList());
    }

    public List<Operator> getHeads() {
        checkAndGenerateGraphView();
        return this.graph.vertexSet().stream()
                .filter(operator -> graph.inDegreeOf(operator) == 0)
                .collect(Collectors.toList());
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getPlatform() {
        return platform;
    }

//    public boolean isEnd(Operator opt) {
//        return tailOpt == opt;
//    }
}
