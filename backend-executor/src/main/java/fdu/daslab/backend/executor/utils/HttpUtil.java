package fdu.daslab.backend.executor.utils;

import fdu.daslab.backend.executor.model.ArgoNode;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * 调用argo server的post接口提交任务
 */
public class HttpUtil {

    /**
     * 提交post请求到指定路径
     *
     * @param url 提交路径
     * @param params post的参数
     * @return response
     */
    public static String postHttp(String url, String params) {
        String responseMsg = "";
        HttpClient httpClient = new HttpClient();
        httpClient.getParams().setContentCharset("UTF-8");
        PostMethod postMethod = new PostMethod(url);
        // 提交的数据，是一个json串
        postMethod.addParameter("data", params);
        try {
            httpClient.executeMethod(postMethod);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = postMethod.getResponseBodyAsStream();
            int len = 0;
            byte[] buf = new byte[1024];
            while((len=in.read(buf))!=-1){
                out.write(buf, 0, len);
            }
            responseMsg = out.toString("UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            postMethod.releaseConnection();
        }
        return responseMsg;
    }

    // argo submit的url
    final String ARGO_URL = "http://localhost:2746/api/v1/workflows/argo";

    /**
     * 调用argo的接口直接提交pipeline
     * @param pipeLine argo需要的pipeline结构
     */
    public static void submitPipeline(List<ArgoNode> pipeLine) {

    }

    /**
     * 指定yaml的路径，直接提交yaml
     *
     * @param yamlPath yaml路径
     */
    public static void submitPipelineByYaml(String yamlPath) {

    }

}
