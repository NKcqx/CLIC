package fdu.daslab.executorcenter.adapter;

import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.base.PlanNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

    /**
     * 把plan的信息以Yaml文件的格式生成，并保存，返回对应的路径地址
     *
     * @param planInfo 该平台的计算信息
     * @return 对应的路径地址
     */
    public String generateYamlForPlan(Plan planInfo) {
        // 遍历 子DAG，把所有opt转为Map保存
        Map<String, Object> yamlMap = graph2Yaml(planInfo);
        String path = dagPrefix + "/physical-dag-" + UUID.randomUUID().toString() + ".yml";
        try {
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            dumperOptions.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            dumperOptions.setPrettyFlow(false);
            Yaml yaml = new Yaml(dumperOptions);
            yaml.dump(yamlMap, new OutputStreamWriter((new FileOutputStream(path))));
        } catch (FileNotFoundException e) {
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
            put("name", opt.name);
            put("id", nodeInfo.nodeId);
            put("params", opt.params);
            put("inputKeys", opt.inputKeys);
            put("outputKeys", opt.outputKeys);
        }};
    }

    // 将operator之间的依赖转化为dag
    private Map<String, Object> operatorDependency2Map(PlanNode nodeInfo, Map<Integer, PlanNode> operators) {
        Map<String, Object> dependencyMap = new HashMap<>(); // 当前Opt的依赖对象
        dependencyMap.put("id", nodeInfo.nodeId);
        if (!nodeInfo.inputNodeId.isEmpty()) {  // head Operator
            List<Map<String, String>> dependencies = new ArrayList<>(); // denpendencies字段，是一个List<Map> 每个元素是其中一个依赖
            for (int i = 0; i < nodeInfo.inputNodeId.size(); i++) {
                int inputId = nodeInfo.inputNodeId.get(i);
                int index = i;
                dependencies.add(new HashMap<String, String>() {{
                    put("id", String.valueOf(inputId));
                    // 恶心的东西，暂时使用上下游的key表示
                    put("sourceKey", operators.get(inputId).operatorInfo.inputKeys.get(0));
                    put("targetKey", nodeInfo.operatorInfo.inputKeys.get(index));
                }});
            }
            dependencyMap.put("dependencies", dependencies);
        }

        return dependencyMap;
    }
}
