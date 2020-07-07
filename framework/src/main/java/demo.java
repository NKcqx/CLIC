import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

public class demo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {

            PlanBuilder planBuilder = new PlanBuilder();
            // 创建节点
            DataQuanta source_node = planBuilder.readDataFrom(new HashMap<String, String>(){{
                put("input", "data/test.csv");
            }});

            DataQuanta filter_node = DataQuanta.createInstance("filter", new HashMap<String, String>(){{
                put("udfName", "filterFunc");
            }});

            DataQuanta map_node = DataQuanta.createInstance("map", new HashMap<String, String>(){{
                put("udfName", "mapFunc");
            }});

            DataQuanta reduce_node = DataQuanta.createInstance("reducebykey", new HashMap<String, String>(){{
                put("udfName", "reduceFunc");
                put("keyName", "reduceKey");
            }});

            DataQuanta sort_node = DataQuanta.createInstance("sort", new HashMap<String, String>(){{
                put("udfName", "sortFunc");
            }});

            DataQuanta sink_node = DataQuanta.createInstance("sink", new HashMap<String, String>(){{
                put("output", "data/output.csv"); // 具体resources的路径通过配置文件获得
            }});

            // 链接节点，即构建DAG
            source_node.outgoing(filter_node, null);

            filter_node.outgoing(map_node, null);

            map_node.outgoing(reduce_node,null);

            reduce_node.outgoing(sort_node, null);

            sort_node.outgoing(sink_node, null);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
