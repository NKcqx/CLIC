package fdu.daslab.executorcenter.adapter;

import fdu.daslab.thrift.base.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * 实现运行时所需要的参数等信息的适配，
 *  包含：Dag的描述文件、容器的参数等
 *
 *      <b>
 *          之前的设计中，对于每个算子，都必须包含inputKeys和outputKeys，为了解决算子多个输入和输出的情况，
 *          这种做法大大增加了系统的复杂性，因为大部分算子都是单个输入或输出，上一个算子的输入就是当前算子的输出。
 *          但是，目前下游的各个平台的实现普遍采用了这样的做法，后面有同学可以考虑将这部分恶心不易用的代码删除
 *      </b>
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/1/21 8:52 PM
 */
@Component
public class ParamAdapter {

    @Value("${dag.prefix}")
    private String dagPrefix; // 输出的dag描述文件的文件存放路径

    @Value("${thrift.notify.host}")
    private String notifyHost;

    @Value("${thrift.notify.port}")
    private int notifyPort;

    // 生成运行的相关参数
    public List<String> wrapperExecutionArguments(Stage stage, Platform platform) {
        // dagPath
        String dagPath = generateYamlForPlan(stage.planInfo);
        // udf，使用不同语言的文件来进行描述
        String udfPath = stage.others.getOrDefault("udfPath-" + platform.language, "");
        // stage相关，包含stageId、jobName、notifyHost、notifyPort
        int stageId = stage.stageId;
        String jobName = stage.jobName;
        // 其他用户传入的参数
        StringBuilder otherParams = new StringBuilder();
        for (String key : stage.others.keySet()) {
            // dev参数表示为了开发方便透传给执行时用的参数，下游一般不需要
            if (!key.startsWith("udfPath") && !key.startsWith("dev")) {
                otherParams.append("--D").append(key).append("=").append(stage.others.get(key));
            }
        }
        // TODO: 通过文件耦合太严重，比如超算等情况，没法使用本地文件处理
        // 使用什么样的格式描述最方便？
        return Arrays.asList(
                "--dagPath=" + dagPath,
                "--udfPath=" + udfPath,
                "--stageId=" + stageId,
                "--jobName=" + jobName,
                "--notifyHost=" + notifyHost,
                "--notifyPort=" + notifyPort,
                otherParams.toString()
        );
    }

    /**
     * 把plan的信息以Yaml文件的格式生成，并保存，返回对应的路径地址
     *
     * @param planInfo 该平台的计算信息
     * @return 对应的路径地址
     */
    private String generateYamlForPlan(Plan planInfo) {
        // 遍历 子DAG，把所有opt转为Map保存
        Map<String, Object> yamlMap = graph2Yaml(planInfo);
        String path = dagPrefix + "/physical-dag-" + UUID.randomUUID().toString() + ".yml";
        try {
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            dumperOptions.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            dumperOptions.setPrettyFlow(false);
            FileOutputStream outputStream = new FileOutputStream(path);
            OutputStreamWriter writer = new OutputStreamWriter(outputStream);
            Yaml yaml = new Yaml(dumperOptions);
            yaml.dump(yamlMap, writer);
            writer.flush();
            writer.close();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return path;
    }

    private Map<String, Object> graph2Yaml(Plan planInfo) {
        // 遍历stage里的dag, 转成YAML字符串
        List<Map<String, Object>> optMapList = new ArrayList<>(); // "operators"字段，是stage里所有opt的列表 YML列表
        List<Map<String, Object>> dagList = new ArrayList<>(); // "dag"字段，各个边的列表

        // 遍历 子DAG，把所有opt转为Map保存
        planInfo.nodes.forEach((nodeId, nodeInfo) -> {
            optMapList.add(operator2Map(nodeInfo));
            dagList.add(operatorDependency2Map(nodeInfo, planInfo.nodes));
        });

        return new HashMap<String, Object>() {{
            put("operators", optMapList);
            put("dag", dagList);
        }};
    }

    // 把一个Operator中的各个属性转化为Map，用于最后将Opt生成YAML
    private Map<String, Object> operator2Map(PlanNode nodeInfo) {
        Operator opt = nodeInfo.operatorInfo;
        return new HashMap<String, Object>() {{
            put("name", opt.name == null ? "" : opt.name);
            put("id", String.valueOf(nodeInfo.nodeId));
            put("params", CollectionUtils.isEmpty(opt.params) ? new HashMap<>() : opt.params);
            put("inputKeys", CollectionUtils.isEmpty(opt.inputKeys) ? new ArrayList<>() : opt.inputKeys);
            put("outputKeys", CollectionUtils.isEmpty(opt.outputKeys) ? new ArrayList<>() : opt.outputKeys);
        }};
    }

    // 将operator之间的依赖转化为dag
    private Map<String, Object> operatorDependency2Map(PlanNode nodeInfo, Map<Integer, PlanNode> operators) {
        Map<String, Object> dependencyMap = new HashMap<>(); // 当前Opt的依赖对象
        dependencyMap.put("id", String.valueOf(nodeInfo.nodeId));
        if (!CollectionUtils.isEmpty(nodeInfo.inputNodeId)) {  // head Operator
            List<Map<String, String>> dependencies = new ArrayList<>(); // denpendencies字段，是一个List<Map> 每个元素是其中一个依赖
            for (int i = 0; i < nodeInfo.inputNodeId.size(); i++) {
                int inputId = nodeInfo.inputNodeId.get(i);
                int index = i;
                dependencies.add(new HashMap<String, String>() {{
                    put("id", String.valueOf(inputId));
                    // 恶心的东西，暂时使用上下游的key表示
                    List<String> inputOutputKeys = operators.get(inputId).operatorInfo.outputKeys;
                    put("sourceKey", CollectionUtils.isEmpty(inputOutputKeys) ? "" :
                            inputOutputKeys.get(0));
                    List<String> targetKeys = nodeInfo.operatorInfo.inputKeys;
                    put("targetKey", CollectionUtils.isEmpty(targetKeys) ? "" :
                            targetKeys.get(index));
                }});
            }
            dependencyMap.put("dependencies", dependencies);
        }

        return dependencyMap;
    }

}
