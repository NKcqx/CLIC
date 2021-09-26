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

@Component("mpiOperator")
public class MPIOperatorStrategy implements KubernetesResourceStrategy {

    @Autowired
    private KubernetesRestClient kubernetesRestClient;

    private Yaml yaml = new Yaml();

    @Value("${kubernetes.mpi.create}")
    private String createMPIUrl;

    @Override
    public void create(Stage stage, Platform platformInfo, List<String> params) throws Exception {
        final InputStream inputStream = new ClassPathResource("templates/mpi-template.yaml").getInputStream();
        // 重新处理参数格式,将第一个等号替换成空格
        for(int i = 0; i < params.size(); i++) {
            params.set(i, params.get(i).replaceFirst("=", " "));
        }
        String templateYaml = StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
        String mpiYaml = templateYaml.replace("$name$", kubernetesRestClient.generateKubernetesName(stage))
                .replace("$image$", stage.others.getOrDefault("mpiImage", platformInfo.defaultImage))
                .replace("$imagePolicy$", stage.others.getOrDefault("dev-imagePolicy", "IfNotPresent"))
                .replace("$nodeNum$", stage.others.getOrDefault("nodeNum", "2"))
                .replace("$mainPath$", platformInfo.params.get("mainPath") + " " + StringUtils.joinWith(" ", params.toArray()))
                .replace("$nfsServer$", platformInfo.params.get("nfsServer"));
        HttpClient httpClient = kubernetesRestClient.getIgnoreHttpClient();
        httpClient.execute(kubernetesRestClient.getDefaultHttpPost(createMPIUrl, yaml.load(mpiYaml)));
    }
}
