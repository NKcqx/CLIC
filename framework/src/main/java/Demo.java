import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class Demo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            PlanBuilder planBuilder = new PlanBuilder("test-web-case");
            // 设置udf路径   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("java", "/data/udfs/TestSmallWebCaseFunc.class");
            //供测试生成文件使用   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("spark", "/data/udfs/TestSmallWebCaseFunc.class");

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "/data/datasets/largeTest.csv");
            }}).withTargetPlatform("java");

            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("spark");

            DataQuanta mapNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapFunc");
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
            }}).withTargetPlatform("spark");

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapNode);
            planBuilder.addVertex(reduceNode);
            planBuilder.addVertex(sortNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, filterNode);
            planBuilder.addEdge(filterNode, mapNode);
            planBuilder.addEdge(mapNode, reduceNode);
            planBuilder.addEdge(reduceNode, sortNode);
            planBuilder.addEdge(sortNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
