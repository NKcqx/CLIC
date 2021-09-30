package fdu.daslab.executorcenter.kubernetes.strategy;

import fdu.daslab.executorcenter.kubernetes.KubernetesResourceStrategy;
import fdu.daslab.executorcenter.kubernetes.KubernetesRestClient;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.base.Stage;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private Logger logger = LoggerFactory.getLogger(MPIOperatorStrategy.class);

    @Autowired
    private KubernetesRestClient kubernetesRestClient;

    private Yaml yaml = new Yaml();

    @Value("${kubernetes.mpi.create}")
    private String createMPIUrl;

    @Override
    public void create(Stage stage, Platform platformInfo, List<String> params) throws Exception {
        final InputStream inputStream = new ClassPathResource("templates/mpi-template.yaml").getInputStream();
        // 重新处理参数格式,将第一个等号替换成空格,以适配cpp的输入参数格式
        for(int i = 0; i < params.size(); i++) {
            params.set(i, params.get(i).replaceFirst("=", " "));
        }
        // TODO: 目前从yaml文件读取的url存在异常，因此如果不是kubeflow的url，会被替换成默认url
        if(!createMPIUrl.split("/")[2].equals("kubeflow.org")) {
            createMPIUrl = "/apis/kubeflow.org/v1/namespaces/mpi-operator/mpijobs";
        }

        String templateYaml = StreamUtils.copyToString(inputStream, StandardCharsets.UTF_8);
        String mpiYaml = templateYaml.replace("$name$", kubernetesRestClient.generateKubernetesName(stage))
                .replace("$image$", stage.others.getOrDefault("mpiImage", platformInfo.defaultImage))
                .replace("$imagePolicy$", stage.others.getOrDefault("dev-imagePolicy", "IfNotPresent"))
                .replace("$nodeNum$", stage.others.getOrDefault("nodeNum", "2"))
                .replace("$mainPath$", platformInfo.params.get("mainPath") + ", " + StringUtils.joinWith(", ", params.toArray()))
                .replace("$nfsServer$", platformInfo.params.get("nfsServer"));

        // 打印信息用于debug
        logger.info("------ MPI job ------");
        logger.info("Create MPI Url: " + createMPIUrl);
        logger.info("Yaml:\n" + mpiYaml);
        HttpClient httpClient = kubernetesRestClient.getIgnoreHttpClient();
        httpClient.execute(kubernetesRestClient.getDefaultHttpPost(createMPIUrl, yaml.load(mpiYaml)));
    }
}