package fdu.daslab.backend.executor.utils;

import org.junit.Before;
import org.junit.Test;
import java.net.URL;

/**
 * Testing for HttpUtil.java
 *
 * @author 唐志伟，刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:41
 */
public class HttpUtilTest {

    private URL url;

    @Before
    public void setUp() {
        url = Thread.currentThread()
                .getContextClassLoader()
                .getResource("templates/argo-dag.yaml");
    }

    @Test
    public void submitPipelineByYaml() {
        assert url != null;
        HttpUtil.submitPipelineByYaml(url.getPath());
    }
}
