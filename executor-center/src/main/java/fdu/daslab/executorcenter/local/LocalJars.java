package fdu.daslab.executorcenter.local;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * 保存本地的底层执行平台的路径地址，每一个平台代码需要先 mvn clean install 到本地的maven仓库
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/11/21 10:41 AM
 */
@Component
public class LocalJars {

    @Value("${mvnDir}")
    private String mvnDir;

    private final Map<String, String> platforms = new HashMap<String, String>() {{
        put("java", "executable-java/1.0-SNAPSHOT/executable-java-1.0-SNAPSHOT-jar-with-dependencies.jar");
        put("spark", "executable-spark/1.0-SNAPSHOT/executable-spark-1.0-SNAPSHOT-jar-with-dependencies.jar");
    }};

    public String getJarForPlatform(String platform) {
        return mvnDir + "/" + platforms.get(platform.toLowerCase());
    }
}
