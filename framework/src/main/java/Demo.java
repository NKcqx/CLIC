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
            // 设置udf路径
            planBuilder.setPlatformUdfPath("java", "/data/TestSmallWebCaseFunc.class");
            //供测试生成文件使用
            planBuilder.setPlatformUdfPath("spark", "/data/TestSmallWebCaseFunc.class");

            // 创建节点
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "/Users/jason/Desktop/test.csv");
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

            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("outputPath", "~/Desktop/output.csv"); // 具体resources的路径通过配置文件获得
            }});

            // 链接节点，即构建DAG
            sourceNode.outgoing(filterNode);

            filterNode.outgoing(mapNode);

            mapNode.outgoing(reduceNode);

            reduceNode.outgoing(sortNode);

            sortNode.outgoing(sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
