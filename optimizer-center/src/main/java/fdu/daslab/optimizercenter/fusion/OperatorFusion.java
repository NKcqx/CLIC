package fdu.daslab.optimizercenter.fusion;

import fdu.daslab.optimizercenter.channel.ChannelEnrich;
import fdu.daslab.thrift.base.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 将同一个平台的多个operator合并到一个stage上
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/20/21 3:28 PM
 */
@Component
public class OperatorFusion {

    @Autowired
    private ChannelEnrich channelEnrich;

    // 创建一个新的stage
    // 从某一个节点开始，遍历，直到遇到source节点或者sink节点
    private Stage addNewStageFromSource(Map<Integer, PlanNode> nodeInfo, PlanNode sourcePlan,
                                        Set<Integer> visited, Map<Integer, Integer> channelToStage) {
        Integer stageId = new Random().nextInt();

        Queue<Integer> Q = new LinkedList<>();
        Q.add(sourcePlan.nodeId);

        List<Integer> sourceId = new ArrayList<>();
        Map<Integer, PlanNode> subPlanNodes = new HashMap<>();

        while (!Q.isEmpty()) {
            Integer curNodeId = Q.poll();
            PlanNode curNode = nodeInfo.get(curNodeId);
            visited.add(curNodeId);
            // 添加当前节点
            subPlanNodes.put(curNodeId, curNode);

            // 如果当前节点不是source节点，则继续向前遍历
            if (!channelEnrich.isSource(curNode) && curNode.inputNodeId != null) {
                for (int preNodeId : curNode.inputNodeId) {
                    if (!visited.contains(preNodeId)) {
                        Q.add(preNodeId);
                    }
                }
            } else {
                // 否则，添加source节点
                sourceId.add(curNodeId);
            }
            // 如果当前节点不是sink节点，则继续向后遍历
            if (!channelEnrich.isSink(curNode) && curNode.outputNodeId != null) {
                for (int postNodeId : curNode.outputNodeId) {
                    if (!visited.contains(postNodeId)) {
                        Q.add(postNodeId);
                    }
                }
            }

            // 保存channel节点
            if (channelEnrich.isChannel(curNode)){
                // 保存channel节点到stageId到映射
                channelToStage.put(curNodeId, stageId);
            }
        }
        Plan subPlan = new Plan(subPlanNodes, sourceId, new HashMap<>());
        Stage stage = new Stage();
        stage.setStageId(stageId);
        stage.setPlanInfo(subPlan);
        String platform = ((PlanNode) subPlan.nodes.values().toArray()[0]).platformName;
        stage.setPlatformName(platform);
        return stage;
    }

    // 连接stage
    private void connectStage(Map<Integer, PlanNode> nodeInfo,
                      Map<Integer, Stage> stages, Map<Integer, Integer> channelToStage) {
        channelToStage.forEach((channelId, stageId) -> {
            Stage stage = stages.get(stageId);
            PlanNode channelNode = nodeInfo.get(channelId);
            // 如果channel节点是source节点，则需要将当前stage的前驱指向channel节点所在前驱的stage
            if (channelEnrich.isSource(channelNode)) {
                new ArrayList<>(channelNode.inputNodeId).forEach(inputId -> {
                    stage.inputStageId.add(channelToStage.get(inputId));
                    // 同时需要将当前前驱从channel中删除
                    channelNode.inputNodeId.remove(inputId);
                });
            } else {
                // 否则，则需要将当前stage的后继指向channel节点所在后继的stage
                new ArrayList<>(channelNode.outputNodeId).forEach(outputId -> {
                    stage.outputStageId.add(channelToStage.get(outputId));
                    channelNode.outputNodeId.remove(outputId);
                });
            }

        });
    }


    /**
     * 将相邻的在一个平台上的operator合并到一个sub plan上，以提升效率
     *
     * 考虑两种情况：
     *      1.全依赖：子图是一个完整的DAG，除了source节点和sink节点之外，内部的节点不依赖外部的其他节点
     *      2.半依赖：子图中间的节点需要依赖外部的输入
     *  不管哪种，fusion方式：
     *      1.直接向和外部交互的节点 添加 channel_sink 和 channel_source 节点；
     *      2.然后，以 channel_sink 和 channel_source 为边界，分隔为不同的 sub plan
     *
     * @param plan 物理执行计划，没有合并相邻的同平台算子
     * @return 合并后的物理执行计划
     */
    public Job fusion(Plan plan) {
        Map<Integer, PlanNode> nodeInfo = plan.getNodes();

        // 标记operator是否被已经被加入stage，因为会存在一个stage中多个source的情况
        Set<Integer> visited = new HashSet<>();
        // 保存 channel节点 到 stageId之间的映射，用于之后连接stage
        Map<Integer, Integer> channelToStage = new HashMap<>();
        // 以channel_source 为界，切分不同的stage
        Map<Integer, Stage> stages = new HashMap<>();

        nodeInfo.forEach((sourceId, sourcePlan) -> {
            if (!visited.contains(sourceId)) {
                Stage stage = addNewStageFromSource(nodeInfo, sourcePlan, visited, channelToStage);
                stages.put(stage.getStageId(), stage);
            }
        });

        // 后处理，包括将stage之间连接起来，以及删除原有的channel_source 和 channel_sink之间的连接
        connectStage(nodeInfo, stages, channelToStage);

        Job fusedPlan = new Job();
        List<Integer> sourceStages = stages.values().stream()
                .filter(stage -> stage.inputStageId.isEmpty())
                .map(Stage::getStageId)
                .collect(Collectors.toList());
        fusedPlan.setSourceStages(sourceStages);
        fusedPlan.setSubplans(stages);

        return fusedPlan;
    }
}
