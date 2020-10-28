package fdu.daslab.backend.executor.utils;

import com.google.common.collect.ImmutableMap;
import fdu.daslab.backend.executor.model.KubernetesStage;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;

/**
 * kubernetes调用需要的工具类，包含创建pod，获取指定pod对应的ip和port
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/28 4:18 PM
 */
public class KubernetesUtil {

    private static String kubeConfigPath; // kubernetes配置地址
    private static String masterIP; // master的ip，需要查询获得
    private static String masterThriftPort; // master启动的thrift端口
    private static String defaultNamespaceName; // 默认的namespace名称

    static {
        try {
            Configuration configuration = new Configuration();
            kubeConfigPath = configuration.getProperty("kube-config-path");
            masterThriftPort = configuration.getProperty("clic-master-port");
            defaultNamespaceName = configuration.getProperty("default-namespace-name");
            // 初始化k8s
            initKubernetes();
            // 查询获取pod-ip
            String masterPodName = configuration.getProperty("clic-master-pod-name"); // master的pod名称
            masterIP = readMasterIP(masterPodName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    // TODO: 未来状态保存到外部之后，master做一个无状态的服务
    // 查询获取pod-ip
    private static String readMasterIP(String masterPodName) {
        try {
            CoreV1Api api = new CoreV1Api();
            // 查询获取pod-ip
            V1Pod masterPod = api.readNamespacedPodStatus(masterPodName, defaultNamespaceName, null);
            return Objects.requireNonNull(masterPod.getStatus()).getPodIP();
        } catch (ApiException e) {
            e.printStackTrace();
        }
        return "localhost";
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
//        client.setDebugging(true);
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
//    private static Map<Integer, KubernetesStage> submitToKubernetes(Map<Integer, KubernetesStage> stagePods) {
//        // 创建一个api
//        CoreV1Api api = new CoreV1Api();
//        // 直接创建若干pod，并同时查询对应赌物理信息，主要是ip和端口
//        stagePods.forEach((stageId, kubernetesStage) -> {
//            try {
//                if (kubernetesStage.getPodInfo() != null) {
//                    api.createNamespacedPod(defaultNamespaceName, kubernetesStage.getPodInfo(), null, null, null);
//                    // 等待，直到pod正在运行中，超过一定时间，则直接报错
//                    V1Pod v1Pod = api.readNamespacedPodStatus(podPrefix + stageId, defaultNamespaceName, null);
//                    int retryCounts = 0;
//                    while (!"Running".equals(Objects.requireNonNull(v1Pod.getStatus()).getPhase())) {
//                        Thread.sleep(1000);
//                        retryCounts++;
//                        if (retryCounts >= 30) {
//                            LOGGER.error("Cannot initialize pod of stage: " + stageId);
//                            return;
//                        }
//                        v1Pod = api.readNamespacedPodStatus(podPrefix + stageId, defaultNamespaceName, null);
//                    }
//                    kubernetesStage.setStageId(stageId);
//                    kubernetesStage.setHost(Objects.requireNonNull(v1Pod.getStatus()).getPodIP());
//                    kubernetesStage.setPort(defaultThriftPort);
//                }
//            } catch (ApiException | InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//        return stagePods;
//    }

    /**
     * 使用java api 创建job
     * @param v1Job job
     */
    public static void submitJobStage(V1Job v1Job) {
        final BatchV1Api batchV1Api = new BatchV1Api();
        try {
            batchV1Api.createNamespacedJob(defaultNamespaceName, v1Job, null, null, null);
        } catch (ApiException e) {
            e.printStackTrace();
        }
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
     * 按照默认方式去创建job，每个stage会单独去创建一个job，下面方式都写死，未来可能收敛到某一个base-template中
     *
     * @param stageId        stage的标识
     * @param containerName  container名字
     * @param containerImage container的image
     * @param containerArgs  image的参数
     * @return V1Pod
     */
    public static V1Job createV1JobByDefault(String stageId, String containerName,
                                             String containerImage, String containerArgs) {
        return new V1JobBuilder()
                .withNewMetadata()
                    .withName(stageId)
                    .withNamespace(defaultNamespaceName)
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withName(containerName)
                                .withImage(containerImage)
                                .withImagePullPolicy("IfNotPresent")
                                .withCommand("/bin/sh", "-c")
                                .withArgs(containerArgs)
                                .addNewVolumeMount()
                                    .withName("nfs-volume")
                                    .withMountPath("/data")
                                .endVolumeMount()
                            .endContainer()
                            .withRestartPolicy("Never")
                            .addNewVolume()
                                .withName("nfs-volume")
                                .withPersistentVolumeClaim(new V1PersistentVolumeClaimVolumeSourceBuilder()
                                        .withClaimName("pvc-nfs").build())
                            .endVolume()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    /**
     * 增加一些额外的参数信息，包含stageId, masterHost, masterPort，基本是用来和master进行交互
     *
     * @param stageId stage的id
     * @return 参数的字符串，格式是 --arg1=xxx --arg2=xxx
     */
    public static String enrichContainerArgs(String stageId) {
        StringBuilder result = new StringBuilder();
        Map<String, String> argsMap = ImmutableMap.of(
                "--stageId", stageId,
                "--masterHost", masterIP,
                "--masterPort", String.valueOf(masterThriftPort));
        for (Map.Entry<String, String> arg : argsMap.entrySet()) {
            result.append(arg.getKey()).append("=").append(arg.getValue()).append(" ");
        }
        return result.toString();
    }

    /**
     * 将argo的yaml适配生成kubernetes client的meta信息
     *
     * @param planName plan的全局唯一的名称
     * @param argoYamlPath argo yaml的路径
     * @return 所有该plan下的stage
     */
    @SuppressWarnings("unchecked")
    public static Map<String, KubernetesStage> adaptArgoYamlToKubernetes(String planName, String argoYamlPath) {
        Yaml argoYaml = new Yaml();
        Map<String, KubernetesStage> resultStages = new HashMap<>();
        try {
            Object loadObj = argoYaml.load(new FileInputStream(argoYamlPath));
            // 获取所有的dag信息及其依赖
            Map<String, Object> specInfo = ((Map<String, Object>) ((Map<String, Object>) loadObj).get("spec"));
            List<Map<String, Object>> templates = (List<Map<String, Object>>) specInfo.get("templates");
            // 默认第一个为workflow的template
            Map<String, Object> dagInfo = (Map<String, Object>) templates.get(0).get("dag");
            List<Map<String, Object>> tasks = (List<Map<String, Object>>) dagInfo.get("tasks");

            // 存储所有template和其container-image的映射关系
            Map<String, String> templateToImage = new HashMap<>();
            templates.forEach(template -> {
                Map<String, Object> container = (Map<String, Object>) template.getOrDefault(
                        "container", new HashMap<>());
                String imageName = (String) container.getOrDefault("image", "");
                templateToImage.put((String) template.get("name"), imageName);
            });

            // stageName到stageId的映射
            Map<String, String> stageNameToStageId = new HashMap<>();
            for (int stageId = 1; stageId <= tasks.size(); stageId++) {
                Map<String, Object> curStage = tasks.get(stageId - 1);
                String uniqueStageId = StringUtils.join(new String[]{
                        planName, (String) curStage.get("template"), String.valueOf(stageId)}, "-");
                stageNameToStageId.put((String) curStage.get("name"), uniqueStageId);
            }
            tasks.forEach(stageTemplate -> buildKubernetesPods(
                    resultStages, stageNameToStageId, templateToImage, stageTemplate));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return resultStages;
    }

    /**
     * 构建KubernetesStage
     *
     * @param resultStages 需要返回的stages
     * @param stageNameToStageId stage的name到全局唯一的stageId的映射
     * @param templateToImage containerName到对应的image的映射
     * @param stageTemplate yaml描述上的每一个template
     */
    @SuppressWarnings("unchecked")
    private static void buildKubernetesPods(Map<String, KubernetesStage> resultStages,
                                            Map<String, String> stageNameToStageId, Map<String, String> templateToImage,
                                            Map<String, Object> stageTemplate) {
        String stageName = (String) stageTemplate.get("name"); // 上层需要保证这个name是唯一的
        String stageId = stageNameToStageId.get(stageName);
        KubernetesStage stage;
        if (resultStages.containsKey(stageId)) {
            stage = resultStages.get(stageId);
        } else {
            stage = new KubernetesStage(stageId);
        }
        // 添加parent和child
        List<String> parentStages = new ArrayList<>();
        Object dependencies = stageTemplate.get("dependencies");
        if (dependencies != null) {
            List<String> dependencyNames = (List<String>) dependencies;
            dependencyNames.forEach(dependency -> {
                String parentId = stageNameToStageId.get(dependency);
                parentStages.add(parentId);
                KubernetesStage parentStage;
                if (resultStages.containsKey(parentId)) {
                    parentStage = resultStages.get(parentId);
                } else {
                    parentStage = new KubernetesStage(parentId);
                    parentStage.setStageId(parentId);
                }
                // 设置parent的child
                parentStage.getChildStageIds().add(stageId);
                resultStages.put(parentId, parentStage);
            });
        }
        stage.getParentStageIds().addAll(parentStages);
        // 添加详细的信息，几个必须的参数: podName, containerName, containerImage, containerArgs
        String containerName = (String) stageTemplate.get("template");
        String containerImage = templateToImage.get(containerName);
        // 正常的args之外，还需要添加一些辅助的参数，比如stageId等信息，这些信息需要手动地动态设置
        String containerArgs = ((List<Map<String, String>>)
                ((Map<String, Object>) stageTemplate.get("arguments")).get("parameters"))
                .get(0).get("value") + " " + enrichContainerArgs(stageId);
        V1Job job = createV1JobByDefault(stageId, containerName,
                containerImage, containerArgs);
        stage.setJobInfo(job);
        stage.setPlatformName(containerName.substring(0, containerName.indexOf("-")));
        resultStages.put(stageId, stage);
    }

}
