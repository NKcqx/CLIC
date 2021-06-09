package fdu.daslab.operatorcenter.init;

import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.OperatorStructure;
import org.apache.commons.io.FileUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 初始化operator，从文件中加载
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/8/21 9:05 PM
 */
@Component
public class OperatorInit {

    @SuppressWarnings("unchecked")
    public Map<String, Operator> init() {
        Map<String, Operator> result = new HashMap<>();
        try {
            File templateFile = new ClassPathResource("init/logical_operators.yaml").getFile();
            String templateYaml = FileUtils.readFileToString(templateFile, StandardCharsets.UTF_8);
            Yaml yaml = new Yaml();
            Map<String, List<Map<String, Object>>> loaded = yaml.load(templateYaml);
            List<Map<String, Object>> operatorList = loaded.get("operators");
            for (Map<String, Object> operatorInfo : operatorList) {
                Operator operator = new Operator();
                operator.setName((String) operatorInfo.get("name"));
                operator.setOutputKeys((List<String>) operatorInfo.getOrDefault("outputKeys",
                        Collections.singletonList("result")));
                operator.setInputKeys((List<String>) operatorInfo.getOrDefault("inputKeys",
                        Collections.singletonList("data")));
                // 参数先使用占位符占位
                List<String> paramsMeta = (List<String>) operatorInfo.getOrDefault("params", new ArrayList<>());
                Map<String, String> params = new HashMap<>();
                for (String key : paramsMeta) {
                    params.put(key, "_");
                }
                operator.setParams(params);
                operator.setOperatorStructure(OperatorStructure.valueOf((String)
                        operatorInfo.getOrDefault("operatorStructure", "MAP")));
                result.put(operator.name, operator);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
