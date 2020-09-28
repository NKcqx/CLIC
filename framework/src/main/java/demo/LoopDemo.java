package demo;

import api.DataQuanta;
import api.PlanBuilder;
import org.javatuples.Pair;

import java.io.StringWriter;
import java.util.HashMap;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/28 10:19 下午
 */
public class LoopDemo {
    public static void main(String[] args) throws Exception{
        PlanBuilder planBuilder = new PlanBuilder();
        planBuilder.setPlatformUdfPath("java", "/Users/jason/Desktop/TestLoopFunc.class");

        // 创建节点   例如该map的value值是本项目test.csv的绝对路径
        DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("inputPath", "/Users/jason/Desktop/fakeInputFile.csv");
        }});

        DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
            put("outputPath", "/Users/jason/Desktop/fakeOutputFile.csv"); // 具体resources的路径通过配置文件获得
        }});

        DataQuanta collectionSource = DataQuanta.createInstance("collection-source", new HashMap<String, String>(){{
            put("inputList", "1");
        }});

        PlanBuilder loopBodyBuilder = new PlanBuilder();
        planBuilder.setPlatformUdfPath("java", "/Users/jason/Desktop/TestLoopFunc.class");
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

        planBuilder.execute();
    }
}
