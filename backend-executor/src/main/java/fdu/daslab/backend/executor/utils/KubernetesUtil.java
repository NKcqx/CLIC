package fdu.daslab.backend.executor.utils;

import com.google.common.collect.ImmutableMap;
import fdu.daslab.backend.executor.model.KubernetesStage;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * kubernetes调用需要的工具类，包含创建pod，获取指定pod对应的ip和port
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/28 4:18 PM
 */
public class KubernetesUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesUtil.class);
    private static String kubeConfigPath; // kubernetes配置地址
    private static Integer defaultThriftPort; // 默认设置的thrift port地址
    private static String podPrefix;  // pod的名称前缀
    private static String driverPodName; // driver的pod名称
    private static String defaultNamespaceName; // 默认的namespace名称

    static {
        try {
            Configuration configuration = new Configuration();
            kubeConfigPath = configuration.getProperty("kube-config-path");
            defaultThriftPort = Integer.valueOf(configuration.getProperty("default-thrift-port"));
            podPrefix = configuration.getProperty("pod-prefix");
            driverPodName = System.getenv("HOSTNAME");
            defaultNamespaceName = configuration.getProperty("default-namespace-name");
            // 初始化k8s
            initKubernetes();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 初始化kubernetes
    public static void initKubernetes() {
        //加载k8s, config
        ApiClient client;
        try {
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream inputStream = classloader.getResourceAsStream(kubeConfigPath);
            assert inputStream != null;
            final KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new InputStreamReader(inputStream));
            client = ClientBuilder.kubeconfig(kubeConfig).build();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        //将加载config的client设置为默认的client
        io.kubernetes.client.openapi.Configuration.setDefaultApiClient(client);
    }

//    /**
//     * 根据argo的文件，生成同时执行的一系列pod，同时保存其物理上的信息（ip, port）并返回
//     *
//     * @param argoPath argo的Dag路径
//     * @return stageId和stageInfo之间的对应关系
//     */
//    public static Map<Integer, KubernetesStage> createStagePodAndGetStageInfo(String argoPath) {
//        // 设置spec信息和依赖信息 TODO：半依赖时如何设置
//        Map<Integer, KubernetesStage> stagePods = YamlUtil.adaptArgoYamlToKubernetes(argoPath);
//        // 设置ip和port等物理信息，并同时提交pod的生成
//        return submitToKubernetes(stagePods);
//    }

//    public static void main(String[] args) {
//        createStagePodAndGetStageInfo("/Users/edward/Code/Lab/data/job-1416858673428874.yml");
//    }

    // 将yaml文件提交到kubernetes集群，并查询对应的stage信息
    private static Map<Integer, KubernetesStage> submitToKubernetes(Map<Integer, KubernetesStage> stagePods) {
        // 创建一个api
        CoreV1Api api = new CoreV1Api();
        // 直接创建若干pod，并同时查询对应赌物理信息，主要是ip和端口
        stagePods.forEach((stageId, kubernetesStage) -> {
            try {
                if (kubernetesStage.getPodInfo() != null) {
                    api.createNamespacedPod(defaultNamespaceName, kubernetesStage.getPodInfo(), null, null, null);
                    // 等待，直到pod正在运行中，超过一定时间，则直接报错
                    V1Pod v1Pod = api.readNamespacedPodStatus(podPrefix + stageId, defaultNamespaceName, null);
                    int retryCounts = 0;
                    while (!"Running".equals(Objects.requireNonNull(v1Pod.getStatus()).getPhase())) {
                        Thread.sleep(1000);
                        retryCounts++;
                        if (retryCounts >= 30) {
                            LOGGER.error("Cannot initialize pod of stage: " + stageId);
                            return;
                        }
                        v1Pod = api.readNamespacedPodStatus(podPrefix + stageId, defaultNamespaceName, null);
                    }
                    kubernetesStage.setStageId(stageId);
                    kubernetesStage.setHost(Objects.requireNonNull(v1Pod.getStatus()).getPodIP());
                    kubernetesStage.setPort(defaultThriftPort);
                }
            } catch (ApiException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        return stagePods;
    }

//    /**
//     * 删除所有已经完成的pod
//     *
//     * @param completedStageIds 已经完成的stage的id列表
//     */
//    public static void deleteCompletedPods(Set<Integer> completedStageIds) {
//        CoreV1Api api = new CoreV1Api();
//        completedStageIds.forEach(stageId -> {
//            try {
//                api.deleteNamespacedPod(podPrefix + stageId, defaultNamespaceName, null,
//                        null, null, null, null, null);
//            } catch (Exception e) {
//                // 此api有bug，但是仍然能够成功删除，暂时忽略错误
//                LOGGER.info(e.getMessage());
//            }
//        });
//    }

    /**
     * 按照默认方式去创建pod，下面方式都写死，为了可能收敛到某一个base-template中
     *
     * @param stageId        stage的标识
     * @param containerName  container名字
     * @param containerImage container的image
     * @param containerArgs  image的参数
     * @return V1Pod
     */
    public static V1Pod createV1PodByDefault(String stageId, String containerName,
                                             String containerImage, String containerArgs) {
        return new V1PodBuilder()
                .withNewMetadata()
                .withName(stageId)
                .withNamespace(defaultNamespaceName)
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName(containerName)
                .withImage(containerImage)
                .withImagePullPolicy("IfNotPresent")
                .withCommand("/bin/sh", "-c")
                .withArgs(containerArgs)
                .addNewEnv()
                .withName("POD_IP")
                .withValueFrom(new V1EnvVarSourceBuilder()
                    .withFieldRef(new V1ObjectFieldSelectorBuilder()
                    .withFieldPath("status.podIP").build()).build())
                .endEnv()
                .addNewEnv()
                .withName("POD_NAME")
                .withValueFrom(new V1EnvVarSourceBuilder()
                    .withFieldRef(new V1ObjectFieldSelectorBuilder()
                    .withFieldPath("metadata.name").build()).build())
                .endEnv()
                .addNewVolumeMount()
                .withName("nfs-volume")
                .withMountPath("/data")
                .endVolumeMount()
                .endContainer()
                .addNewVolume()
                .withName("nfs-volume")
                .withPersistentVolumeClaim(new V1PersistentVolumeClaimVolumeSourceBuilder()
                        .withClaimName("pvc-nfs").build())
                .endVolume()
                .endSpec()
                .build();
    }

    /**
     * 增加一些额外的参数信息，包含stageId, thriftPort, driverHost, driverPort，基本是用来和driver进行交互
     *
     * @param stageId stage的id
     * @return 参数的字符串，格式是 --arg1=xxx --arg2=xxx
     */
    public static String enrichContainerArgs(String stageId) {
        CoreV1Api api = new CoreV1Api();
        StringBuilder result = new StringBuilder();
        // 获取driver的ip
        String driverHost = "";
        try {
            V1Pod driverPod = api.readNamespacedPodStatus(driverPodName, defaultNamespaceName, null);
            driverHost = Objects.requireNonNull(driverPod.getStatus()).getPodIP();
        } catch (ApiException e) {
            e.printStackTrace();
        }
        // driverPort 和 thriftPort 暂时都使用统一的默认值
        assert driverHost != null;
        Map<String, String> argsMap = ImmutableMap.of(
                "--stageId", stageId,
                "--port", String.valueOf(defaultThriftPort),
                "--driverHost", driverHost,
                "--driverPort", String.valueOf(defaultThriftPort));
        for (Map.Entry<String, String> arg : argsMap.entrySet()) {
            result.append(arg.getKey()).append("=").append(arg.getValue()).append(" ");
        }

        return result.toString();
    }

}
