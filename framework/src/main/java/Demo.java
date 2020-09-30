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
            PlanBuilder planBuilder = new PlanBuilder();
            // 设置udf路径   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            //planBuilder.setPlatformUdfPath("java", "/data/TestSmallWebCaseFunc.class");
            planBuilder.setPlatformUdfPath("java", "/Users/zhuxingpo/Downloads/Due/TestSmallWebCaseFunc.class");
            //供测试生成文件使用   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            //planBuilder.setPlatformUdfPath("spark", "/data/TestSmallWoebCaseFunc.class");
            planBuilder.setPlatformUdfPath("spark", "/Users/zhuxingpo/Downloads/Due/TestSmallWoebCaseFunc.class");

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                //put("inputPath", "hdfs://hdfs-namenode:8020/zxpDir/test.csv");
                put("inputPath", "hdfs:///localhost:8020/input/test.csv");
            }});

            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }});

            DataQuanta mapNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapFunc");
            }});

            DataQuanta reduceNode = DataQuanta.createInstance("reducebykey", new HashMap<String, String>() {{
                put("udfName", "reduceFunc");
                put("keyName", "reduceKey");
            }});

            DataQuanta sortNode = DataQuanta.createInstance("sort", new HashMap<String, String>() {{
                put("udfName", "sortFunc");
            }});

            // 最终结果的输出路径   例如该map的value值是本项目output.csv的绝对路径
            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                //put("outputPath", "hdfs://hdfs-namenode:8020/zxpDir/output.csv"); // 具体resources的路径通过配置文件获得
                put("outputPath", "hdfs:///localhost:8020/output/output.csv"); // 具体resources的路径通过配置文件获得
            }});

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapNode);
            planBuilder.addVertex(reduceNode);
            planBuilder.addVertex(sortNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, filterNode, null);
            planBuilder.addEdge(filterNode, mapNode, null);
            planBuilder.addEdge(mapNode, reduceNode, null);
            planBuilder.addEdge(reduceNode, sortNode, null);
            planBuilder.addEdge(sortNode, sinkNode, null);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
