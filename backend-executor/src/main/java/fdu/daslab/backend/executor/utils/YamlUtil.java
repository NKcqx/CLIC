package fdu.daslab.backend.executor.utils;
import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.ImageTemplate;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 写入yaml的工具类
 *
 *  @author 杜清华
 *  @since  2020/7/6 11:39
 *  @version 1.0
 */
public class YamlUtil {

    private static String argoDag = YamlUtil.class.getClassLoader().
            getResource("templates/argo-dag-simple.yaml").getPath();

    private static String resPath = System.getProperty("user.dir") + "/backend-executor/src/main/resources/result/job-";

    /**
     * 根据pipeline生成yaml，并保存，返回保存的路径
     *
     * @param tasks argoNodes
     * @param imageTemplates image列表
     * @return 生成路径
     */
    public String createArgoYaml(List<ArgoNode> tasks, List<ImageTemplate> imageTemplates) {

        String resultPath = joinYaml(tasks, imageTemplates);

        return resultPath;
    }

    /**
     * 读取dag模板以及template并进行拼接
     *
     * @param nodes argonodes
     * @param imageTemplates image列表
     * @return 拼接完成的yaml路径
     */
    public String joinYaml(List<ArgoNode> nodes, List<ImageTemplate> imageTemplates) {
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
            // 首先获取该node对应的template配置
            ImageTemplate imageTemplate = imageTemplates.stream()
                    .filter(template -> template.getPlatform().equals(node.getPlatform()))
                    .collect(Collectors.toList())
                    .get(0);
            tasks.add(joinTask(node, imageTemplate));
            tpl.add(node.getPlatform());
        }
        dag.put("tasks", tasks);
        //根据node中所需要的template去加载相应的Template
        for (String platform : tpl) {
            templates.add(readYaml(TemplateUtil.getTemplatePathByPlatform(platform)));
        }
        long n = System.nanoTime();
        String storePath = resPath + n + ".yaml";
        //存入指定路径
        writeYaml(storePath, argoDagMap);

        return storePath;
    }

    /**
     * 拼接task部分
     *
     * @param node argo节点
     * @param imageTemplate 模版
     * @return task map
     */
    public Map joinTask(ArgoNode node, ImageTemplate imageTemplate) {
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

        paraName.put("name", node.getPlatform() + "Args");

        String parameterStr = imageTemplate.getParamPrefix() + " "; // 运行字符串前缀
        for (ArgoNode.Parameter parameter : node.getParameters()) {
            parameterStr += parameter.getName() + "=" + parameter.getValue() + " ";
        }
        paraValue.put("value", parameterStr);

        paraMap.putAll(paraName);
        paraMap.putAll(paraValue);
        paraList.add(paraMap);
        arguPara.put("parameters", paraList);
        taskArgu.put("arguments", arguPara);
        // template需要查询然后获得，传入的明确来说应该是platform，而不是template
        String templateName = TemplateUtil.getOrCreateTemplate(imageTemplate);
        taskTem.put("template", templateName);
        taskName.put("name", node.getName());

        taskMap.putAll(taskName);

        //判断是否加dependencies，暂定的依赖判断逻辑是：
        //取当前node的id和dependencies，然后取dependencies中id-1位置的node即为其依赖（先考虑单个依赖）
        List<ArgoNode> dep = node.getDependencies();
        if (dep.get(dep.size() - 1) != null) { //存在依赖，则添加dependencies
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
