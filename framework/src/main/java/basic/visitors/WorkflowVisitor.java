package basic.visitors;

import basic.Stage;
import basic.operators.Operator;
import basic.operators.OperatorEntity;
import channel.Channel;
import org.jgrapht.Graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * StageVisitor
 * 1. 遍历Physical Plan 在识别到跨平台时，插入SourceOperator和SinkOperator（这步以后还要选择source\sink，会放到别的Visitor中）
 * 2. 将同一个平台下的sub-plan组织为stage(dag)，设置为ArgoNode,做法类似ArgoAdapter中的setArgoNode
 * 3. 把tasks（所有的Argo Node）用YamlUtil转为YAML（或者YAMLVisitor，这一步应该放到Planbuilder中做）
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/20 10:14 上午
 */
public class WorkflowVisitor extends Visitor {
    private List<Stage> stages = new ArrayList<>(); // stage列表
    private Stage curStage = null;
    private OperatorEntity curOptPlatform = null;
    private Integer jobID = 1;
    private Graph<Operator, Channel> graph;

    public WorkflowVisitor(Graph graph) {
        super();
        this.graph = graph;
    }

    public List<Stage> getStages() {
        return this.stages;
    }

    @Override
    public void visit(Operator opt) {
        // 拓扑排序不会出现重复访问同一元素的情况，无需判断visited
        if (curOptPlatform == null) {
            curOptPlatform = opt.getSelectedEntities();
            curStage = new Stage(String.valueOf(jobID),
                    "Stage-" + opt.getOperatorName(),
                    curOptPlatform.getEntityID(),
                    this.graph);
            // curStage.setHead(opt);
            curStage.addVertex(opt);
        }
        // 找到所有运算平台 和 自己运算平台相同的 下一跳Opt
        Set<Channel> outgoingChannels = graph.outgoingEdgesOf(opt).stream().filter(channel ->
                graph.getEdgeTarget(channel)
                        .getSelectedEntities()
                        .getEntityID()
                        .equals(curOptPlatform.getEntityID())).collect(Collectors.toSet());
        if (outgoingChannels.isEmpty()){
            // 没有相同平台的Opt了，将当前opt设为stage 的 tail 然后开始组装下一个stage
            stages.add(curStage);
            ++this.jobID;
            curOptPlatform = null;

        }else {
            curStage.addEdges(outgoingChannels); // todo 其实有问题：还没有添加下一跳节点的时候就把相连的边添加进来了
        }
        if (planTraversal.hasNextOpt()) {

            if (nextOpt == null) {
                // 没有相同平台的Opt了，将当前opt设为stage 的 tail 然后开始组装下一个stage
                curStage.setTail(opt);
                stages.add(curStage);
                this.jobID++;
                curOptPlatform = null;
                // 重新随便找一个下一跳
                nextOpt = planTraversal.nextOpt();
                // 遍历下一跳
                nextOpt.acceptVisitor(this);
            } else {
                // 遍历下一跳
                nextOpt.acceptVisitor(this);
            }
        } else {
            curStage.setTail(opt);
            stages.add(curStage);
        }
    }

}
