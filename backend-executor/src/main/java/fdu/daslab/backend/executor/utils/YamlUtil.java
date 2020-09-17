package fdu.daslab.backend.executor.utils;

import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.ImageTemplate;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 写入yaml的工具类
 *
 * @author 杜清华，陈齐翔
 * @version 1.0
 * @since 2020/7/6 11:39
 */
public class YamlUtil {

    private static String argoDag = Objects.requireNonNull(YamlUtil.class.getClassLoader().
            getResource("templates/argo-dag-simple.yaml")).getPath();

    private static String resJobPath;

    public static String getResPltDagPath() {
        return resPltDagPath;
    }

    private static String resPltDagPath;

    static {
        try {
            Configuration configuration = new Configuration();
            resJobPath = configuration.getProperty("yaml-output-path") + configuration.getProperty("yaml-prefix");
            resPltDagPath = configuration.getProperty("yaml-output-path");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据pipeline生成yaml，并保存，返回保存的路径
     *
     * @param tasks          argoNodes
     * @param imageTemplates image列表
     * @return 生成路径
     */
    public String createArgoYaml(List<ArgoNode> tasks, List<ImageTemplate> imageTemplates) {
        return joinYaml(tasks, imageTemplates);
    }

    /**
     * 读取dag模板以及template并进行拼接
     *
     * @param nodes          argonodes
     * @param imageTemplates image列表
     * @return 拼接完成的yaml路径
     */
    private String joinYaml(List<ArgoNode> nodes, List<ImageTemplate> imageTemplates) {
        List<Object> tasks = new LinkedList<>();
        List<String> tpl = new ArrayList<>();

        Map<String, Object> argoDagMap = readYaml(argoDag);
        @SuppressWarnings("unchecked")
        Map<Object, Object> spec = (Map<Object, Object>) argoDagMap.get("spec");
        @SuppressWarnings("unchecked")
        List<Object> templates = (List<Object>) spec.get("templates");
        @SuppressWarnings("unchecked")
        Map<Object, Object> dagTemp = (Map<Object, Object>) templates.get(0);
        @SuppressWarnings("unchecked")
        Map<Object, Object> dag = (Map<Object, Object>) dagTemp.get("dag");
        //添加task
        for (ArgoNode node : nodes) {
            // 首先获取该node对应的template配置
            ImageTemplate imageTemplate = imageTemplates.stream()
                    .filter(template -> template.getPlatform().equals(node.getPlatform()))
                    .collect(Collectors.toList())
                    .get(0);
            if (!tpl.contains(node.getPlatform())) {
                tpl.add(node.getPlatform());
            }

            tasks.add(joinTask(node, imageTemplate));

        }
        dag.put("tasks", tasks);
        //根据node中所需要的template去加载相应的Template
        for (String platform : tpl) {
            templates.add(readYaml(TemplateUtil.getTemplatePathByPlatform(platform)));
        }
        long n = System.nanoTime();
        String storePath = resJobPath + n + ".yml";
        //存入指定路径
        writeYaml(storePath, argoDagMap);

        return storePath;
    }

    /**
     * 拼接task部分
     *
     * @param node          argo节点
     * @param imageTemplate 模版
     * @return task map
     */
    private Map joinTask(ArgoNode node, ImageTemplate imageTemplate) {
        Map<String, Object> taskName = new HashMap<>();
        Map<String, Object> taskTem = new HashMap<>();
        Map<String, Object> taskArgu = new HashMap<>();
        Map<String, Object> taskMap = new HashMap<>();
        Map<String, Object> taskDep = new HashMap<>();
        Map<String, Object> arguPara = new HashMap<>();
        List<Map<String, Object>> paraList = new LinkedList<>();
        Map<String, Object> paraMap = new HashMap<>();
        Map<String, Object> paraName = new HashMap<>();
        Map<String, Object> paraValue = new HashMap<>();

        paraName.put("name", node.getPlatform() + "Args");

        String parameterStr = imageTemplate.getParamPrefix() + " "; // 运行字符串前缀
        Map<String, String> params = node.getParameters(); // yml格式的字典型参数对象
        /*// map -> yaml string
        Yaml yaml = new Yaml();
        StringWriter stringWriter = new StringWriter();
        yaml.dump(params, stringWriter);*/
        StringBuilder stringBuilder = new StringBuilder();
        params.forEach((key, value) -> {
            stringBuilder.append(key).append("=").append(value);
        });
        parameterStr += stringBuilder.toString();
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
        //判断是否加dependencies，暂定的依赖判断逻辑是：
        //取当前node的id和dependencies，然后取dependencies中id-1位置的node即为其依赖（先考虑单个依赖）
        List<ArgoNode> deps = node.getDependencies();
        List<String> depsName = new ArrayList<>();

        if (deps != null && !deps.isEmpty() && deps.get(0) != null) { //存在依赖，则添加dependencies
            deps.forEach(dep -> {
                depsName.add(dep.getName());
            });
            taskDep.put("dependencies", depsName);
            taskMap.putAll(taskDep);
        }
        taskMap.putAll(taskName);
        taskMap.putAll(taskTem);
        taskMap.putAll(taskArgu);
        return taskMap;
    }

    /**
     * 读取yaml
     *
     * @param path 需要读取的文件路径
     * @return 读取结果
     */
    private Map<String, Object> readYaml(String path) {
        Map<String, Object> m = null;
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
     */
    public static void writeYaml(String path, Map<String, Object> res) {
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
    }

}
