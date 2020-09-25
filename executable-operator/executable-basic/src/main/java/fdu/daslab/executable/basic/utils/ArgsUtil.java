package fdu.daslab.executable.basic.utils;

import com.beust.jcommander.JCommander;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.OperatorFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * 参数解析的工具类
 *
 * @author 唐志伟, 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:36 PM
 */
public class ArgsUtil {

    public static UUID randomUUID(){
        return UUID.randomUUID();
    }

    /**
     * 通过某个key参数分割参数集合，并按顺序返回
     *
     * @param args   参数列表
     * @param keyArg 关键字
     * @return 有序组织的 关键字对应的value - 参数列表
     */
    public static Map<String, String[]> separateArgsByKey(String[] args, String keyArg) {
        // 需要保证插入顺序和实际顺序一致
        Map<String, String[]> separateMaps = new LinkedHashMap<>();
        int i = 0;
        while (i < args.length && args[i].startsWith(keyArg)) {
            // 下一个分割位置
            int nextIndex = i + 1;
            while (nextIndex < args.length && !args[nextIndex].startsWith(keyArg)) {
                nextIndex++;
            }
            separateMaps.put(args[i].substring(keyArg.length() + 1), Arrays.copyOfRange(args, i + 1, nextIndex));
            i = nextIndex;
        }
        return separateMaps;
    }


    /**
     * 1. 解析YAML文件
     * 2. 根据'opt'字段创建所有Opt，赋值后放到Map<ID, Opt>里
     * 3. 根据'dag'字段从Map（根据ID）拿到两端的Opt并相连, 最后返回头节点
     *
     * @param yamlPath yaml文件的路径
     * @return DAG的头节点(返回头节点还是尾节点呢 ? ）
     */
    public static OperatorBase parseArgs(String yamlPath, OperatorFactory factory) throws Exception {
        InputStream yamlStream = new FileInputStream(new File(yamlPath));
        Yaml yaml = new Yaml();
        Map<String, Object> yamlArgs = yaml.load(yamlStream);
        Map<String, OperatorBase> operatorPool =
                parseOpt((List<Map<String, Object>>) yamlArgs.get("operators"), factory);
        OperatorBase headOperator =
                parseDependency((List<Map<String, Object>>) yamlArgs.get("dag"), operatorPool);
        return headOperator;
    }

    /**
     * 实例化yaml参数中的各个Opt，并赋予相应参数值
     *
     * @param operatorArgs 所以Opt相关的参数
     * @return 所有Opt的字典，ID为Key，Operator为Value
     */
    private static Map<String, OperatorBase> parseOpt(
            List<Map<String, Object>> operatorArgs,
            OperatorFactory factory)
            throws Exception {
        Map<String, OperatorBase> operatorPool = new HashMap<>();

        // 实体化参数中的所有Opt，并放入Map中
        for (Map<String, Object> optArg : operatorArgs) {
            String name = (String) optArg.get("name");
            String id = (String) optArg.get("id");
            List<String> inputKeys = (List<String>) optArg.get("inputKeys");
            List<String> outputKeys = (List<String>) optArg.get("outputKeys");
            Map<String, String> params = (Map<String, String>) optArg.get("params");
            OperatorBase operatorBase = factory.createOperator(name, id, inputKeys, outputKeys, params);
            operatorPool.put(id, operatorBase);
        }
        return operatorPool;
    }

    /**
     * 链接各个Operator，组织为Task（DAG）并找到标明头节点
     *
     * @param dagArgs      传入的yaml字符串形式的dag，列表中每个元素代表dag里的一条边
     * @param operatorPool 所有的Operator
     * @return 头节点
     */
    private static OperatorBase parseDependency(List<Map<String, Object>> dagArgs,
                                                Map<String, OperatorBase> operatorPool) {
        OperatorBase headOpt = null;
        for (int i = 0; i < dagArgs.size(); ++i) {
            Map<String, Object> arg = dagArgs.get(i);
            // 当前Opt，因为dag里的依赖关系是自底向上构建的，所以当前Opt即为targetOpt
            OperatorBase targetOperator = operatorPool.get(arg.get("id"));
            List<Map<String, String>> dependencies = (List<Map<String, String>>) arg.get("dependencies");
            if (dependencies != null) { // 头节点没有依赖边
                for (Map<String, String> dependency : dependencies) {
                    String sourceKey = dependency.get("sourceKey");
                    String targetKey = dependency.get("targetKey");
                    OperatorBase sourceOperator = operatorPool.get(dependency.get("id"));
                    // 设置双指针链接
                    sourceOperator.connectTo(sourceKey, targetOperator, targetKey);
                    targetOperator.connectFrom(targetKey, sourceOperator, sourceKey);
                }
            }
        }
        headOpt = operatorPool.values().stream()
                .filter(operatorBase -> operatorBase.getInputConnections().size() == 0)
                .findAny().get();
        return headOpt;
    }

    /**
     * 解析命令行参数
     *
     * @param argObject 解析到的对象
     * @param args      输入参数
     */
    public static void parseArgs(Object argObject, String[] args) {
        JCommander.newBuilder()
                .addObject(argObject)
                .build()
                .parse(args);
    }
}
