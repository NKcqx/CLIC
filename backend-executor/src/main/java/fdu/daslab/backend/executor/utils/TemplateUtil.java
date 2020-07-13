package fdu.daslab.backend.executor.utils;

import fdu.daslab.backend.executor.model.ImageTemplate;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.InputStream;
import java.util.*;

/**
 * 容器模版相关工具类
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/7 3:39 PM
 */
public class TemplateUtil {

    // template的名称后缀
    private static final String TEMPLATE_SUFFIX = "-template";
    // template的存放路径
    private static String templateDir = Objects.requireNonNull(TemplateUtil.class.getClassLoader()
            .getResource("templates/")).getPath();
    // root-template的存放的Stream
    private static InputStream rootTemplateStream = TemplateUtil.class.getClassLoader()
            .getResourceAsStream("templates/root-template.yaml");

    /**
     * 根据平台的名称获取对应的template名称
     *
     * @param platform 平台名称，如java、spark
     * @return 默认是加上一个后缀
     */
    public static String getTemplateNameByPlatform(String platform) {
        return platform + TEMPLATE_SUFFIX;
    }

    /**
     * 根据平台的名称拼装对应的template存储路径
     *
     * @param platform 平台名称，如java、spark
     * @return 默认存放在templates目录下
     */
    public static String getTemplatePathByPlatform(String platform) {
        //由于存在转义问题，暂时先拼接
        String path = templateDir + getTemplateNameByPlatform(platform) + ".yaml";
        return path;
//        return Paths.get(templateDir, getTemplateNameByPlatform(platform) + ".yaml")
//                .toString();
    }

    /**
     * 根据配置信息生成template的yaml描述，先读取该template是否存在，存在直接返回，否则创建
     *
     * @param imageTemplate 配置信息
     * @return 生成的template名称
     */
    public static String getOrCreateTemplate(ImageTemplate imageTemplate) {
        String templateName = getTemplateNameByPlatform(imageTemplate.getPlatform());
        String templatePath = getTemplatePathByPlatform(imageTemplate.getPlatform());
        File templateFile = new File(templatePath);
        if (!templateFile.exists()) {
            // 根据imageTemplate重新创建一个
            Yaml yaml = new Yaml();
            // rootTemplate的yaml作为模版
            Map<String, Object> templateMap = yaml.load(rootTemplateStream);
            templateMap.put("name", templateName);
            templateMap.put("inputs", new HashMap<String, List<Map<String, String>>>() {{
                put("parameters", new ArrayList<Map<String, String>>() {{
                    add(new HashMap<String, String>() {{
                        // 这个是依照之前写入yaml的规定写的
                        put("name", imageTemplate.getPlatform() + "Args");
                    }});
                }});
            }});
            // 写入container
            @SuppressWarnings("unchecked")
            Map<String, Object> templateContainer = (Map<String, Object>) templateMap.get("container");
            templateContainer.put("image", imageTemplate.getImage());
            templateContainer.put("command", imageTemplate.getCommand());
            // args暂时没有用途，和之前定义的绑定即可
            templateContainer.put("args", new ArrayList<String>() {{
                add("{{inputs.parameters." + imageTemplate.getPlatform() + "Args}}");
            }});
            templateMap.put("container", templateContainer);

            // 写入文件
            YamlUtil yamlUtil = new YamlUtil();
            yamlUtil.writeYaml(templatePath, templateMap);
        }
        return templateName;
    }
}
