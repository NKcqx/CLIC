package demo;

import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/18 11:16
 */
public class TestCrimeDataDemo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            PlanBuilder planBuilder = new PlanBuilder("test-crime");
            planBuilder.setPlatformUdfPath("java", "/data/udfs/TestCrimeDataFunc.class");
            planBuilder.setPlatformUdfPath("spark", "/data/udfs/TestCrimeDataFunc.class");

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "/data/datasets/london_crime.csv");
            }}).withTargetPlatform("spark");

            DataQuanta mapCateNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapCateFunc");
            }}).withTargetPlatform("spark");

            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("spark");

            DataQuanta mapMonthNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapMonthFunc");
            }}).withTargetPlatform("java");

            DataQuanta reduceNode = DataQuanta.createInstance("reduce-by-key", new HashMap<String, String>() {{
                put("udfName", "reduceFunc");
                put("keyName", "reduceKey");
            }}).withTargetPlatform("java");

            DataQuanta sortNode = DataQuanta.createInstance("sort", new HashMap<String, String>() {{
                put("udfName", "sortFunc");
            }}).withTargetPlatform("java");

            // 最终结果的输出路径   例如该map的value值是本项目output.csv的绝对路径
            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("outputPath", "/data/datasets/output.csv"); // 具体resources的路径通过配置文件获得
            }}).withTargetPlatform("java");

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(mapCateNode);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapMonthNode);
            planBuilder.addVertex(reduceNode);
            planBuilder.addVertex(sortNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, mapCateNode);
            planBuilder.addEdge(mapCateNode, filterNode);
            planBuilder.addEdge(filterNode, mapMonthNode);
            planBuilder.addEdge(mapMonthNode, reduceNode);
            planBuilder.addEdge(reduceNode, sortNode);
            planBuilder.addEdge(sortNode, sinkNode);
            planBuilder.addEdge(reduceNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
