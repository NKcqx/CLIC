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

@Component("MPIOperator")
public class MPIOperatorStrategy implements KubernetesResourceStrategy {

    @Autowired
    private KubernetesRestClient kubernetesRestClient;

    private Yaml yaml = new Yaml();

    @Value("${kubernetes.mpi.create}")
    private String createMPIUrl;

    @Override
    public void create(Stage stage, Platform platformInfo, List<String> params) throws Exception {
        final InputStream inputStream = new ClassPathResource("templates/mpi-template.yaml").getInputStream();
        String templateYaml = StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
        String mpiYaml = templateYaml.replace("$name$", kubernetesRestClient.generateKubernetesName(stage))
                .replace("$image$", platformInfo.defaultImage)
                .replace("$imagePolicy$", stage.others.getOrDefault("dev-imagePolicy", "IfNotPresent"))
                .replace("$nodeNum$", platformInfo.params.get("nodeNum"))
                .replace("$mainPath$", platformInfo.params.get("mainPath"))
                .replace("$nfsServer$", platformInfo.params.get("nfsServer"));
        HttpClient httpClient = kubernetesRestClient.getIgnoreHttpClient();
        httpClient.execute(kubernetesRestClient.getDefaultHttpPost(createMPIUrl, yaml.load(mpiYaml)));
    }
}
