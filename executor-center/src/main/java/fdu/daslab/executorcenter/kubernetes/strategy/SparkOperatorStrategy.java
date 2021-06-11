package fdu.daslab.executorcenter.kubernetes.strategy;

import fdu.daslab.executorcenter.kubernetes.KubernetesResourceStrategy;
import fdu.daslab.executorcenter.kubernetes.KubernetesRestClient;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.base.Stage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/6/21 3:09 PM
 */
@Component("sparkOperator") // 格式为 平台 + Operator
public class SparkOperatorStrategy implements KubernetesResourceStrategy {

    @Autowired
    private KubernetesRestClient kubernetesRestClient;

    private Yaml yaml = new Yaml();

    @Value("${kubernetes.spark.create}")
    private String createSparkUrl;

    @Override
    public void create(Stage stage, Platform platformInfo, List<String> params) {
        try {
            File templateFile = new ClassPathResource("templates/spark-template.yaml").getFile();
            String templateYaml = FileUtils.readFileToString(templateFile, StandardCharsets.UTF_8);
            String sparkYaml = templateYaml.replace("$name", kubernetesRestClient.generateKubernetesName(stage))
                    .replace("$image", platformInfo.defaultImage)
                    .replace("$mainClass", platformInfo.params.get("mainClass"))
                    .replace("$mainJar", platformInfo.params.get("mainJar"))
                    .replace("$sparkVersion", platformInfo.params.get("sparkVersion"))
                    .replace("$argument", StringUtils.join(params));
            HttpClient httpClient = kubernetesRestClient.getIgnoreHttpClient();
            httpClient.execute(kubernetesRestClient.getDefaultHttpPost(createSparkUrl, yaml.load(sparkYaml)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
