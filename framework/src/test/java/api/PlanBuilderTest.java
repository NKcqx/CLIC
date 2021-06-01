package api;


import basic.Configuration;
import basic.operators.Operator;
import basic.visitors.Visitor;
import channel.Channel;
import org.javatuples.Pair;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class PlanBuilderTest {
    private PlanBuilder planBuilder;
    private Configuration configuration;

    @Before
    public void before() throws Exception {
        configuration = new Configuration();
        planBuilder = new PlanBuilder(configuration);

    }

    @Test
    public void testConstructor() throws Exception {
        Configuration spyConfiguration = spy(configuration);
        planBuilder = new PlanBuilder(spyConfiguration);
        verify(spyConfiguration).getProperty("operator-mapping-file");
        verify(spyConfiguration).getProperty("platform-mapping-file");
    }

    @Test
    public void testAddMethod() throws Exception {
        Pair<String, String> keyPair = new Pair<>("input_key", "output_key");
        List<Pair<String, String>> keyPairs = new ArrayList<>();
        keyPairs.add(keyPair);
        Operator operator = new Operator("optId", "optName", "calculator");
        DataQuanta dataQuanta1 = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("inputPath", "fake path");
        }});
        DataQuanta dataQuanta2 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "udfNameValue");
        }});
        // test
        assertTrue(planBuilder.addVertex(dataQuanta1));
        assertTrue(planBuilder.addVertex(dataQuanta2));
        ;
        assertTrue(planBuilder.addVertex(operator));
        assertTrue(planBuilder.addEdge(dataQuanta1, dataQuanta2, keyPair));
        // 要运行testExecute需要先把这行给注释掉，否则不会产生一个DAG
        //assertTrue(planBuilder.addEdges(dataQuanta2, dataQuanta1, keyPairs));
    }


    @Test
    public void buildLoopPlanTest() throws Exception {
        PlanBuilder planBuilder = new PlanBuilder();
        planBuilder.setPlatformUdfPath("java", "TestLoopFunc.class");

        // 创建节点   例如该map的value值是本项目test.csv的绝对路径
        DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "/Users/jason/Desktop/fakeInputFile.csv");
        }});

        DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
            put("outputPath", "/fakeOutputFile.csv"); // 具体resources的路径通过配置文件获得
        }});

        DataQuanta collectionSource = DataQuanta.createInstance("collection-source", new HashMap<String, String>() {{
            put("inputList", "1");
        }});

        PlanBuilder loopBodyBuilder = new PlanBuilder();
        planBuilder.setPlatformUdfPath("java", "TestLoopFunc.class");
        DataQuanta loopBodyMap = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "loopBodyMapFunc");
        }});
        loopBodyBuilder.addVertex(loopBodyMap);
        StringWriter stringWriter = new StringWriter();
        loopBodyBuilder.toYaml(stringWriter);

        DataQuanta loopNode = DataQuanta.createInstance("loop", new HashMap<String, String>() {{
            put("predicateName", "loopCondition");
            put("loopBody", stringWriter.toString());
            put("loopVarUpdateName", "increment");
        }});

        planBuilder.addVertex(loopNode);
        planBuilder.addVertex(sourceNode);
        planBuilder.addVertex(sinkNode);
        planBuilder.addVertex(collectionSource);

        planBuilder.addEdge(sourceNode, loopNode);
        planBuilder.addEdge(collectionSource, loopNode, new Pair<>("result", "loopVar"));
        planBuilder.addEdge(loopNode, sinkNode);

        // planBuilder.execute();
    }

    @Test
    public void testReadFromData() throws Exception {
        // 这一步实际上是创建了一个空的Configuration
        //输入的这对String就是HeadDataQuanta
        DataQuanta source = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "data/test.csv");
        }});
        Assert.assertNotNull(planBuilder.getHeadDataQuanta());
        Assert.assertEquals(source, planBuilder.getHeadDataQuanta().get(0));
    }

    @Test
    public void buildMultiSourceDag() throws Exception {
        PlanBuilder planBuilder = new PlanBuilder();
        DataQuanta sourceA = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "/Users/jason/Desktop/fakeInputFileA.csv");
        }});

        DataQuanta sourceB = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "/Users/jason/Desktop/fakeInputFileB.csv");
        }});

        List<DataQuanta> heads = planBuilder.getHeadDataQuanta();
        Assert.assertTrue(heads.contains(sourceA));
        Assert.assertTrue(heads.contains(sourceB));

        planBuilder.optimizePlan();
    }

    @Test
    public void optimizeTwoIsolatePlanTest() throws Exception {
        PlanBuilder planBuilder = new PlanBuilder();
        DataQuanta sourceA = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "/Users/jason/Desktop/fakeInputFileA.csv");
        }});

        DataQuanta sourceB = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "/Users/jason/Desktop/fakeInputFileB.csv");
        }});

        DataQuanta mapA = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "fake");
        }});

        DataQuanta mapB = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "fake");
        }});

        planBuilder.addVertex(sourceA);
        planBuilder.addVertex(sourceB);
        planBuilder.addVertex(mapA);
        planBuilder.addVertex(mapB);

        planBuilder.addEdge(sourceA, mapA);
        planBuilder.addEdge(sourceB, mapB);
        planBuilder.optimizePlan();

        TopologicalOrderIterator<Operator, Channel> topologicalOrderIterator = new TopologicalOrderIterator<>(planBuilder.getGraph());

        while (topologicalOrderIterator.hasNext()) {
            topologicalOrderIterator.next().acceptVisitor(new Visitor() {
                @Override
                public void visit(Operator opt) {
                    Assert.assertNotNull(opt.getSelectedEntities());
                }
            });
        }

    }
}
