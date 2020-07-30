package fdu.daslab.backend.executor.model;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Testing for ArgoNode.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:42
 */
public class ArgoNodeTest {

    private List<ArgoNode.Parameter> params;
    private ArgoNode node;

    @Before
    public void setUp() {
        List<ArgoNode> dependencies = new ArrayList<>();
        int idInJob = 0;
        idInJob += 1;
        ArgoNode nodeZero = new ArgoNode(idInJob, "Job-java-SortFilterReduceByKey386-0",
                "spark", null, null);
        dependencies.add(nodeZero);

        params = new ArrayList<>();
        int randomNumber = (int) Math.round(Math.random() * 1000);
        String platform = "java";
        List<String> optName = new ArrayList<>();
        optName.add("SourceOperator");
        optName.add("MapOperator");
        optName.add("SinkOperator");
        String nameSub = "";
        if (optName.size() > 3) {
            int i = 0;
            for (String n : optName) {
                i++;
                nameSub = nameSub + n.substring(0, 2);
                if (i > 5) {
                    break;
                }
            }
        } else {
            for (String n : optName) {
                nameSub = nameSub + n.replace("Operator", "");
            }
        }
        String name = "Job-" + platform + "-" + nameSub + randomNumber + "-" + idInJob;
        idInJob += 1;
        node = new ArgoNode(idInJob, name, platform, dependencies, null);
        // 在每个node中加上 source 和 sink 算子
        // 分别用于读取和保存各个node的输入和输出
        params.add(new ArgoNode.Parameter("--operator", "SourceOperator"));
        params.add(new ArgoNode.Parameter("--input", "data/" + (idInJob - 1) + ".csv"));
        params.add(new ArgoNode.Parameter("--separator", ","));

        params.add(new ArgoNode.Parameter("--operator", "MapOperator"));

        params.add(new ArgoNode.Parameter("--operator", "SinkOperator"));
        params.add(new ArgoNode.Parameter("--output", "data/" + idInJob + ".csv"));
        params.add(new ArgoNode.Parameter("--separator", ","));
    }

    @Test
    public void getParameters() {
        node.setParameters(params);
        List<ArgoNode.Parameter> nodeParams = node.getParameters();

        String[] expectedNames = {
                "--operator", "--input", "--separator", "--operator", "--operator", "--output", "--separator"
        };
        String[] names = new String[7];
        String[] expectedValues = {
                "SourceOperator", "data/1.csv", ",", "MapOperator", "SinkOperator", "data/2.csv", ","
        };
        String[] values = new String[7];

        for (int i = 0; i < nodeParams.size(); i++) {
            names[i] = nodeParams.get(i).getName();
            values[i] = nodeParams.get(i).getValue();
        }

        assertArrayEquals(expectedNames, names);
        assertArrayEquals(expectedValues, values);
    }

    @Test
    public void getId() {
        assertEquals(2, node.getId());
    }

    @Test
    public void getName() {
        String actualName = node.getName().substring(0, 22) + "-0";
        assertEquals("Job-java-SourceMapSink-0", actualName);
    }

    @Test
    public void getPlatform() {
        assertEquals("java", node.getPlatform());
    }

    @Test
    public void getDependencies() {
        List<ArgoNode> nodeDependencies = node.getDependencies();
        String[] dependenciesNames = new String[nodeDependencies.size()];
        for (int i = 0; i < nodeDependencies.size(); i++) {
            dependenciesNames[i] = nodeDependencies.get(i).getName();
        }
        String[] expectedNames = {
                "Job-java-SortFilterReduceByKey386-0"
        };
        assertArrayEquals(expectedNames, dependenciesNames);
    }
}
