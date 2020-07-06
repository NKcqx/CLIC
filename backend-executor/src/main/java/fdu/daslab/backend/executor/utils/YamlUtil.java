package fdu.daslab.backend.executor.utils;

import fdu.daslab.backend.executor.model.ArgoNode;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * 写入yaml的工具类
 */
public class YamlUtil {

    private static String argoDag = YamlUtil.class.getClassLoader().getResource("templates/argo-dag-simple.yaml").getPath();
    private static String javaTemplate = YamlUtil.class.getClassLoader().getResource("templates/java-template.yaml").getPath();
    private static String sparkTemplate = YamlUtil.class.getClassLoader().getResource("templates/spark-template.yaml").getPath();

    private static String resPath = System.getProperty("user.dir") + "/backend-executor/src/main/resources/result/job-";

    /**
     * 根据pipeline生成yaml，并保存，返回保存的路径
     *
     * @param tasks argoNodes
     * @return 生成路径
     */
    public String createArgoYaml(List<ArgoNode> tasks) {

        String resPath = joinYaml(tasks);

        return resPath;
    }

    /**
     * 读取dag模板以及template并进行拼接
     *
     * @param nodes argonodes
     * @return 拼接完成的yaml路径
     */
    public String joinYaml(List<ArgoNode> nodes) {
        List tasks = new LinkedList();
        List templatePlt = new LinkedList();
        List<String> tpl = new ArrayList<>();

        Map argoDagMap = readYaml(argoDag);
        Map spec = (Map) argoDagMap.get("spec");
        List templates = (List) spec.get("templates");
        Map dagTemp = (Map) templates.get(0);
        Map dag = (Map) dagTemp.get("dag");
        //添加task
        for (ArgoNode node : nodes) {
            tasks.add(joinTask(node));
            tpl.add(node.getTemplate());
        }
        dag.put("tasks", tasks);
        //根据node中所需要的template去加载相应的Template
        if (tpl.contains("java")) {
            templates.add(readYaml(javaTemplate));
        }
        if (tpl.contains("spark")) {
            templates.add(readYaml(sparkTemplate));
        } //后续可根据平台添加
        long n = System.nanoTime();
        String storePath = resPath + n + ".yaml";
        //存入指定路径
        writeYaml(storePath, argoDagMap);

        return storePath;
    }

    /**
     * 拼接task部分
     *
     * @param node
     * @return task map
     */
    public Map joinTask(ArgoNode node) {

        //List<Map> tasks=new LinkedList<>();
        Map taskName = new HashMap();
        Map taskTem = new HashMap();
        Map taskArgu = new HashMap();
        Map taskMap = new HashMap();
        Map taskDep = new HashMap();
        Map arguPara = new HashMap();
        List<Map> paraList = new LinkedList<>();
        Map paraMap = new HashMap();
        Map paraName = new HashMap();
        Map paraValue = new HashMap();

        paraName.put("name", node.getTemplate() + "Args");


        String parameterStr = "";
        if (node.getTemplate().equals("java")) {
            // TODO: --UDFPath属于Image级别的参数（不属于Opt），这种参数该怎么指定呢？
            parameterStr += "java -jar executable-java.jar --udfPath=/data/TestSmallWebCaseFunc.class ";//java运行封装好的jar
        }
        for (ArgoNode.Parameter parameter : node.getParameters()) {
            parameterStr += parameter.getName() + "=" + parameter.getValue() + " ";
        }
        paraValue.put("value", parameterStr);

        paraMap.putAll(paraName);
        paraMap.putAll(paraValue);
        paraList.add(paraMap);
        arguPara.put("parameters", paraList);
        taskArgu.put("arguments", arguPara);
        taskTem.put("template", node.getTemplate() + "-template");
        taskName.put("name", node.getName());

        taskMap.putAll(taskName);

        //判断是否加dependencies，暂定的依赖判断逻辑是：
        //取当前node的id和dependencies，然后取dependencies中id-1位置的node即为其依赖（先考虑单个依赖）
        List<ArgoNode> dep = node.getDependencies();
        if (dep.get(dep.size() - 1) != null) {//存在依赖，则添加dependencies
            taskDep.put("dependencies", "[" + dep.get(dep.size() - 1).getName() + "]");
            taskMap.putAll(taskDep);
        }
        taskMap.putAll(taskTem);
        taskMap.putAll(taskArgu);
        // tasks.add(taskMap);

        return taskMap;
    }

    /**
     * 读取yaml
     *
     * @param path 需要读取的文件路径
     * @return 读取结果
     */
    public Map readYaml(String path) {
        Map m = null;
        try {
            //设置yaml文件格式
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            dumperOptions.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            dumperOptions.setPrettyFlow(false);
            Yaml yaml = new Yaml(dumperOptions);
            m = yaml.load(new FileInputStream(path));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return m;
    }

    /**
     * @param path 需要写入的文件路径
     * @param res  需要写入yaml的内容
     * @return boolean
     */
    public boolean writeYaml(String path, Map res) {

        try {
            //设置yaml文件格式
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            dumperOptions.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            dumperOptions.setPrettyFlow(false);
            Yaml yaml = new Yaml(dumperOptions);
            yaml.dump(res, new OutputStreamWriter((new FileOutputStream(path))));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

}
