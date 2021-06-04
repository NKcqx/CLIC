package demo;

import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/24 15:06
 */
public class FlinkStreamDemo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            PlanBuilder planBuilder = new PlanBuilder("test-stream");
            planBuilder.setPlatformUdfPath("flink", "D:/study/data/udf/TestHotItemFunc.class");

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readStreamDataFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/study/data/2019-Dec.csv");
            }}).withTargetPlatform("flink");

            DataQuanta filterNode = DataQuanta.createInstance("stream-filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("flink");

            DataQuanta timestampNode = DataQuanta.createInstance("stream-assign-timestamp", new HashMap<String, String>() {{
                put("udfName", "assignTimestampFunc");
            }}).withTargetPlatform("flink");

            DataQuanta mapCateNode = DataQuanta.createInstance("stream-map", new HashMap<String, String>() {{
                put("udfName", "mapCateFunc");
            }}).withTargetPlatform("flink");

            DataQuanta reduceNode = DataQuanta.createInstance("stream-reduce-by-key-in-window", new HashMap<String, String>() {{
                put("udfName", "reduceFunc");
                put("keyName", "reduceKey");
                put("winFunc", "windowFunc");
                put("timeInterval", "8");
                put("timeStep", "4");
            }}).withTargetPlatform("flink");

            DataQuanta topNNode = DataQuanta.createInstance("stream-topN-by-key-in-window", new HashMap<String, String>() {{
                put("id", "topNID");
                put("keyName", "topNKey");
                put("windowEnd", "topNWindow");
                put("topSize", "5");
            }}).withTargetPlatform("flink");

            // 最终结果的输出路径   例如该map的value值是本项目output.csv的绝对路径
            DataQuanta sinkNode = DataQuanta.createInstance("stream-sink", new HashMap<String, String>() {{
                put("outputPath", "D:/study/code/Scala/FlinkTutorial/target/classes/output.txt"); // 具体resources的路径通过配置文件获得
            }}).withTargetPlatform("flink");

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(timestampNode);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapCateNode);
            planBuilder.addVertex(reduceNode);
            planBuilder.addVertex(topNNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, filterNode);
            planBuilder.addEdge(filterNode, timestampNode);
            planBuilder.addEdge(timestampNode, mapCateNode);
            planBuilder.addEdge(mapCateNode, reduceNode);
            planBuilder.addEdge(reduceNode, topNNode);
            planBuilder.addEdge(topNNode, sinkNode);

            planBuilder.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
