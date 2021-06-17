package fdu.daslab.operatorcenter.init;

import fdu.daslab.thrift.base.Platform;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 初始化platform，从文件中加载
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/9/21 9:40 PM
 */
@Component
public class PlatformInit {

    @SuppressWarnings("unchecked")
    public Map<String, Platform> init() {
        Map<String, Platform> result = new HashMap<>();
        try {
            InputStream fileInputStream = new ClassPathResource("init/platform_meta.yaml").getInputStream();
            String templateYaml = StreamUtils.copyToString(fileInputStream, StandardCharsets.UTF_8);
            Yaml yaml = new Yaml();
            Map<String, List<Map<String, Object>>> loaded = yaml.load(templateYaml);
            List<Map<String, Object>> platformList = loaded.get("platforms");
            for (Map<String, Object> platformInfo : platformList) {
                Platform platform = new Platform();
                platform.setName((String) platformInfo.get("name"));
                platform.setLanguage((String) platformInfo.get("language"));
                platform.setDefaultImage((String) platformInfo.get("defaultImage"));
                platform.setUseOperator((boolean) platformInfo.get("useOperator"));
                platform.setExecCommand((String) platformInfo.getOrDefault("execCommand", ""));
                platform.setParams((Map<String, String>) platformInfo.getOrDefault("params", new HashMap<>()));
                result.put(platform.name.toLowerCase(), platform);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
