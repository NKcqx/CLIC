package fdu.daslab.backend.executor.utils;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

public class HttpUtilTest extends TestCase {

    @Test
    public void submitPipelineByYaml() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("templates/argo-dag.yaml");
        assert url!=null;
        HttpUtil.submitPipelineByYaml(url.getPath());
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void postHttp() {
    }

    @Test
    public void testSubmitPipelineByYaml() {
    }
}
