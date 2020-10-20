package api;

import org.javatuples.Pair;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.crypto.Data;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;

/**
 * @Author nathan
 * @Date 2020/7/8 7:07 下午
 * @Version 1.0
 */
public class PlanBuilderTest {
    @Test
    public void testReadFromData() throws Exception {
        PlanBuilder planBuilder = new PlanBuilder();
        assert planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "data/test.csv");
        }}) == planBuilder.getHeadDataQuanta();

        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("inputPath", "data/test.csv");
        }});
        planBuilder.setHeadDataQuanta(dataQuanta);
        assert planBuilder.getHeadDataQuanta() == dataQuanta;
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
}
