package fdu.daslab.optimizercenter.channel;

import fdu.daslab.thrift.base.OperatorStructure;
import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.base.PlanNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * 将相邻的平台的不同算子，选择合并的channel进行连接
 *
 * channel_source 和 channel_sink 是一种特殊的source 和 sink节点
 *      其 operatorStructure是source和sink；
 *      但是 其具有 前驱 和 后继，并且前驱 和 后继指向其上下stage的sink、source节点
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/20/21 3:31 PM
 */
@Component
public class ChannelEnrich {

    @Autowired
    private ChannelInstantiate channelInstantiate;

    // 判断是否是source节点，包括普通的source 和 channel source
    public boolean isSource(PlanNode planNode) {
        return OperatorStructure.SOURCE == planNode.getOperatorInfo().operatorStructure;
    }

    // 判断是否是sink节点，包含普通的sink 和 channel sink
    public boolean isSink(PlanNode planNode) {
        return OperatorStructure.SINK == planNode.getOperatorInfo().operatorStructure;
    }

    // 判断是否是channel节点
    public boolean isChannel(PlanNode planNode) {
        return isSource(planNode) && planNode.inputNodeId != null && planNode.getInputNodeIdSize() > 0
            || isSink(planNode) && planNode.outputNodeId != null && planNode.getOutputNodeIdSize() > 0;
    }

    // 创建一个channel节点，这个标记是临时的，最终会被实例化为一个实际的 source
    private PlanNode createChannelNode(String platformName) {
        Random random = new Random();
        PlanNode channelNode = new PlanNode();
        channelNode.setNodeId(random.nextInt());
        channelNode.setPlatformName(platformName);
        return channelNode;
    }

    // 创建channel_sink 和 channel_source 节点
    public Map<Integer, PlanNode> createChannelNode(PlanNode preNode, PlanNode postNode) {
        Map<Integer, PlanNode> channelNodes = new HashMap<>();

        // 添加channelSink
        PlanNode channelSink = createChannelNode(preNode.platformName);
        // 修改sink的前驱和preNode的后继
        channelSink.setInputNodeId(new ArrayList<>(Collections.singletonList(preNode.nodeId)));
        preNode.outputNodeId.replaceAll(id -> id == postNode.nodeId ? channelSink.nodeId : id);

        // 添加channel source
        PlanNode channelSource = createChannelNode(postNode.platformName);
        // 修改source的后继和postNode的前驱
        channelSource.setOutputNodeId(new ArrayList<>(Collections.singletonList(postNode.nodeId)));
        postNode.inputNodeId.replaceAll(id -> id == preNode.nodeId ? channelSource.nodeId : id);
        // 修改source的前驱和sink的后继
        channelSource.setInputNodeId(new ArrayList<>(Collections.singletonList(channelSink.nodeId)));
        channelSink.setOutputNodeId(new ArrayList<>(Collections.singletonList(channelSource.nodeId)));

        // 实例化source或者sink算子
        channelInstantiate.instantiateSourceSink(channelSource, channelSink);

        channelNodes.put(channelSource.nodeId, channelSource);
        channelNodes.put(channelSink.nodeId, channelSink);
        return channelNodes;
    }

    /**
     *  直接遍历所有的节点，
     *      如果某一个节点的下游和当前节点不在一个平台上，则添加channel在当前节点和下游节点；
     *   注意，还需要修改当前节点和上下游节点的连接给相邻的不同平台的算子之间添加channel节点
     *
     * @param plan 优化好的physical plan
     * @return 添加了channel 节点的plan
     */
    public Plan enrichPlan(Plan plan) {
        Map<Integer, PlanNode> planNodes = plan.nodes;
        List<Integer> sourceIds = plan.sourceNodes;

        Queue<Integer> Q = new LinkedList<>(sourceIds);
        Map<Integer, Integer> inDegree = new HashMap<>();
        for (PlanNode node : planNodes.values()) {
            inDegree.put(node.nodeId, node.getInputNodeIdSize());
        }
        Map<Integer, PlanNode> channelNodes = new HashMap<>();
        // 添加channel node
        while (!Q.isEmpty()) {
            int nodeId = Q.poll();
            PlanNode node = planNodes.get(nodeId);
            String curPlatform = node.platformName;
            // 遍历所有的下游节点
            if (node.outputNodeId != null) {
                for (int outputId : node.outputNodeId) {
                    PlanNode outputNode = planNodes.get(outputId);
                    // 如果平台不一致，则添加channel node
                    if (!curPlatform.equals(outputNode.platformName)) {
                        channelNodes.putAll(createChannelNode(node, outputNode));
                    }

                    // 更新下游的degree
                    inDegree.put(outputId, inDegree.get(outputId) - 1);
                    if (inDegree.get(outputId) == 0) {
                        Q.add(outputId);
                    }
                }
            }
        }
        planNodes.putAll(channelNodes);
        plan.setNodes(planNodes);
        return plan;
    }
}
