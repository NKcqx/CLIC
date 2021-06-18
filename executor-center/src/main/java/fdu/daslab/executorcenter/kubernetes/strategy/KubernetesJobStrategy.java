package fdu.daslab.executorcenter.kubernetes.strategy;

import fdu.daslab.executorcenter.kubernetes.KubernetesResourceStrategy;
import fdu.daslab.executorcenter.kubernetes.KubernetesRestClient;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.base.Stage;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * kubernetes job
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/6/21 3:05 PM
 */
@Component("kubernetesJob")
public class KubernetesJobStrategy implements KubernetesResourceStrategy {

    @Autowired
    private KubernetesRestClient kubernetesRestClient;

    @Value("${kubernetes.job.create}")
    private String createJobUrl;

    private Yaml yaml = new Yaml();

    @Override
    public void create(Stage stage, Platform platformInfo, List<String> params) throws Exception {
        // 读取job的模版文件，然后使用对应的替换
        final InputStream inputStream = new ClassPathResource("templates/job-template.yaml").getInputStream();
        String templateYaml = StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);

        String jobYaml = templateYaml.replace("$name$", kubernetesRestClient.generateKubernetesName(stage))
                .replace("$platform$", stage.platformName.toLowerCase())
                .replace("$image$", platformInfo.defaultImage)
                .replace("$imagePolicy$", stage.others.getOrDefault("imagePolicy", "IfNotPresent"))
                .replace("$commands$", platformInfo.execCommand + " " + StringUtils.joinWith(" ", params.toArray()));
        HttpClient httpClient = kubernetesRestClient.getIgnoreHttpClient();
        // 可能会执行失败，需要加一些错误处理
        httpClient.execute(kubernetesRestClient.getDefaultHttpPost(createJobUrl, yaml.load(jobYaml)));
    }

}
