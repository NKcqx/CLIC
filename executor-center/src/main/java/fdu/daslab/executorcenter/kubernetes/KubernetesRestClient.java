package fdu.daslab.executorcenter.kubernetes;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;


/**
 * 访问kubernetes的客户端
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/2/21 4:07 PM
 */
@Component
public class KubernetesRestClient {

    @Value("${kubernetes.token}")
    private String token; // 访问的token

    @Value("${kubernetes.api-server}")
    private String apiServer;    // 集群的APIServer

    // 忽略ssl的客户端
    public HttpClient getIgnoreHttpClient() {
        SSLContextBuilder builder = new SSLContextBuilder();
        SSLConnectionSocketFactory sslsf = null;
        try {
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            sslsf = new SSLConnectionSocketFactory(
                    builder.build());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return HttpClients.custom().setSSLSocketFactory(sslsf).build();
    }

    // HttpHandler for Get
    public HttpGet getDefaultHttpGet(String url) {
        HttpGet httpGet = new HttpGet("https://" + apiServer + url);
        httpGet.addHeader("Authorization", "Bearer " + token);
        return httpGet;
    }

    // HttpHandler For Post
    public HttpPost getDefaultHttpPost(String url, Map<String, Object> objectMap) {
        Gson gson = new Gson();
        HttpPost httpPost = new HttpPost("https://" + apiServer + url);
        httpPost.addHeader("Authorization", "Bearer " + token);
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("Accept", "application/json, */*");
        httpPost.setEntity(new StringEntity(gson.toJson(objectMap), "utf-8"));
        return httpPost;
    }

    // 获取返回的json对象
    public JsonObject getJsonObject(HttpResponse response) {
        JsonObject result = null;
        if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try {
                    String entityString = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                    result = JsonParser.parseString(entityString).getAsJsonObject();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        return result;

    }

}
