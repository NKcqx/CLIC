package fdu.daslab.optimizercenter.channel;

import fdu.daslab.optimizercenter.client.OperatorClient;
import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.PlanNode;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.UUID;

/**
 * 给定两个不同平台的channel source node 和 channel sink node，选择最优的source node
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/23/21 2:50 PM
 */
@Component
public class ChannelInstantiate {

    @Resource
    private OperatorClient operatorClient;

    /**
     * 将 channel_source 和 channel_sink 实例化为 最优的 sink / source 节点；
     *      首先 需要选择两个平台共有的source / sink算子；
     *      然后再 选择效率最高的source sink
     *
     * TODO: 实现相关的channel 选择
     *
     * @param sourceNode channelSource
     * @param sinkNode channelSink
     */
    public void instantiateSourceSink(PlanNode sourceNode, PlanNode sinkNode) {
        // 如果是文件的话，就需要将文件匹配起来
        String filePath = UUID.randomUUID().toString();

        Operator sourceOperator = new Operator(), sinkOperator = new Operator();
        try {
            operatorClient.open();
            sourceOperator = operatorClient.getClient().findOperatorInfo("SourceOperator", null);
            sinkOperator = operatorClient.getClient().findOperatorInfo("SinkOperator", null);
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            operatorClient.close();
        }
        sourceOperator.params.put("inputPath", filePath);
        sinkOperator.params.put("outputPath", filePath);
        sourceNode.setOperatorInfo(sourceOperator);
        sinkNode.setOperatorInfo(sinkOperator);
    }
}
