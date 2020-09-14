package basic;

import basic.operators.Operator;
import channel.Channel;
import org.jgrapht.Graph;
import org.jgrapht.graph.AsSubgraph;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

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

    public AsSubgraph<Operator, Channel> getGraph(){
        if (graph == null){
            graph = new AsSubgraph<>(baseGraph, vertexSet, edgeSet);
        }
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

//    public Operator getTail() {
//        return tailOpt;
//    }
//
//    public void setTail(Operator tail) {
//        this.tailOpt = tail;
//    }
//
//    public void setHead(Operator head) {
//        this.headOpt = head;
//    }
//
//    public Operator getHead() {
//        return headOpt;
//    }

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
