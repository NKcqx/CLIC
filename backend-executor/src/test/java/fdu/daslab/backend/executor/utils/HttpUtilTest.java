package fdu.daslab.backend.executor.utils;

import org.junit.Test;

import java.net.URL;

public class HttpUtilTest {

    @Test
    public void submitPipelineByYaml() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("templates/argo-dag.yaml");
        assert url!=null;
        HttpUtil.submitPipelineByYaml(url.getPath());
    }
}
