package api;

import basic.Configuration;
import basic.operators.Operator;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

public class PlanBuilderTest {
    private PlanBuilder planBuilder;
    @Before
    public void before() throws Exception{
        planBuilder = new PlanBuilder();
    }

    @Test
    public void testConstructor() throws Exception {
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getProperty("operator-mapping-file")).thenReturn("OperatorTemplates/OperatorMapping.xml");
        when(mockConf.getProperty("platform-mapping-file")).thenReturn("Platform/PlatformMapping.xml");
        planBuilder = new PlanBuilder(mockConf);
        verify(mockConf).getProperty("operator-mapping-file");
        verify(mockConf).getProperty("platform-mapping-file");
    }

    @Test
    public void testAddMethod() throws Exception{
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
        assertTrue(planBuilder.addVertex(dataQuanta2));;
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

        DataQuanta collectionSource = DataQuanta.createInstance("collection-source", new HashMap<String, String>(){{
            put("inputList", "1");
        }});

        PlanBuilder loopBodyBuilder = new PlanBuilder();
        planBuilder.setPlatformUdfPath("java", "TestLoopFunc.class");
        DataQuanta loopBodyMap = DataQuanta.createInstance("map", new HashMap<String, String>(){{
            put("udfName", "loopBodyMapFunc");
        }});
        loopBodyBuilder.addVertex(loopBodyMap);
        StringWriter stringWriter = new StringWriter();
        loopBodyBuilder.toYaml(stringWriter);

        DataQuanta loopNode = DataQuanta.createInstance("loop", new HashMap<String, String>(){{
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
        assertEquals(planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "data/test.csv");
        }}),planBuilder.getHeadDataQuanta());

        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("inputPath", "data/test.csv");
        }});
        planBuilder.setHeadDataQuanta(dataQuanta);
        assertEquals(planBuilder.getHeadDataQuanta(), dataQuanta);
    }

    @Test
    public void testExecute() throws Exception{
        // 几个Plan在Execute中调用的次数测试
        Configuration configuration = new Configuration();
        planBuilder = new PlanBuilder(configuration);
        PlanBuilder spyPlanBuilder = spy(planBuilder);
        testAddMethod();
        spyPlanBuilder.execute();
        verify(spyPlanBuilder, times(2)).printPlan();
        verify(spyPlanBuilder).optimizePlan();
        // executePlan是一个private，测试不了
//        verify(spyPlanBuilder).executePlan();
    }
}