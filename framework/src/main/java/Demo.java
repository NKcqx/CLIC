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
            // 创建节点
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("input", "data/test.csv");
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

                //put("is_reverse", "false");
                put("udfName", "sortFunc");
            }});

            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("output", "data/output.csv"); // 具体resources的路径通过配置文件获得
            }});

            // 链接节点，即构建DAG
            sourceNode.outgoing(filterNode, null);

            filterNode.outgoing(mapNode, null);

            mapNode.outgoing(reduceNode, null);

            reduceNode.outgoing(sortNode, null);

            sortNode.outgoing(sinkNode, null);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
