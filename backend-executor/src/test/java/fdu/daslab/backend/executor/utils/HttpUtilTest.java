package fdu.daslab.backend.executor.utils;

import org.junit.Test;

public class HttpUtilTest {

    @Test
    public void submitPipelineByYaml() {
        HttpUtil.submitPipelineByYaml("/Users/edward/Code/Lab/IRDemo/backend-executor/src/main/resources/templates/argo-dag.yaml");
    }
}
