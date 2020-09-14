package fdu.daslab.backend.executor.model;

import fdu.daslab.backend.executor.utils.YamlUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Testing for ArgoNode.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:42
 */
public class ArgoNodeTest {
    private ArgoNode argoNode;

    @Before
    public void setUp() {
        UUID id = UUID.randomUUID();
        argoNode = new ArgoNode(String.valueOf(id), "Stage-" + "FilterOperator", "java", null);
        // 遍历stage里的dag, 转成YAML字符串

        String path = YamlUtil.getResPltDagPath() + "physical-dag-" + argoNode.getId() + ".yml";
        argoNode.setParameters(new HashMap<String, String>() {{
                 put("--dagPath", path);
        }});
    }

    @Test
    public void getParameters() {
        Map<String, String> nodeParams = argoNode.getParameters();
        for (Map.Entry<String, String> entry : nodeParams.entrySet()) {
            assertEquals("--dagPath", entry.getKey());
            assertEquals("/tmp/irdemo_output/physical-dag-1595888266.yml",
                    entry.getValue().substring(0, 32) + "1595888266.yml");
        }
    }

    @Test
    public void getName() {
        assertEquals("Stage-FilterOperator", argoNode.getName());
    }

    @Test
    public void getPlatform() {
        assertEquals("java", argoNode.getPlatform());
    }
}
