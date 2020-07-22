package fdu.daslab.backend.executor.utils;

import com.google.gson.Gson;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * 调用argo server的post接口提交任务
 *
 * @author 唐志伟
 * @since 2020/7/6 1:59 PM
 * @version 1.0
 */
public class HttpUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);

    // argo server的配置路径
    private static final String ARGO_SERVER_CONFIG = "argo-server.yaml";

    /**
     * 提交post请求到指定路径
     *
     * @param url    提交路径
     * @param params post的参数
     */
    public static void postHttp(String url, String params) {
        HttpClient httpClient = new HttpClient();
        httpClient.getParams().setContentCharset("UTF-8");
        PostMethod postMethod = new PostMethod(url);

        try {
            // 提交的数据，是一个json串
            RequestEntity requestEntity = new StringRequestEntity(
                    params, "application/json", "UTF-8");
            postMethod.setRequestEntity(requestEntity);
            postMethod.setRequestHeader("Content-Type", "application/json");
            httpClient.executeMethod(postMethod);
            String responseMsg = postMethod.getResponseBodyAsString();
            LOGGER.info(responseMsg);
        } catch (IOException e) {
//            e.printStackTrace();
            LOGGER.error(e.getMessage());
        } finally {
            postMethod.releaseConnection();
        }
    }

    /**
     * 指定yaml的路径，直接提交yaml
     *
     * @param yamlPath yaml路径
     */
    public static void submitPipelineByYaml(String yamlPath) {
        Yaml yaml = new Yaml();
        // 读取yamlPath的文件
        File yamlFile = new File(yamlPath);
        String yamlJson = "";
        try (InputStream inputStream = new FileInputStream(yamlFile)) {
            Map<String, Object> yamlObject = yaml.load(inputStream);
            // 包装下
            Map<String, Object> wrappedObject = new HashMap<>();
            wrappedObject.put("namespace", "argo");
            wrappedObject.put("serverDryRun", false);
            wrappedObject.put("workflow", yamlObject);
            Gson gson = new Gson();
            yamlJson = gson.toJson(wrappedObject);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 获取argo server的配置路径
        InputStream argoServerStream = HttpUtil.class.getClassLoader()
                .getResourceAsStream(ARGO_SERVER_CONFIG);
        Map<String, Object> yamlObject = yaml.load(argoServerStream);
        @SuppressWarnings("unchecked")
        Map<String, Object> serverPathMap = (Map<String, Object>) yamlObject.get("argoServer");
        String argoPath = serverPathMap.get("url") + ":" + serverPathMap.get("port").toString()
                + serverPathMap.get("submitPath");
        // 提交请求
        postHttp(argoPath, yamlJson);
    }

}
