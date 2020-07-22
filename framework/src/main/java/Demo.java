import api.DataQuanta;
import api.PlanBuilder;
import java.util.HashMap;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class Demo {
    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();
            // 设置udf路径
            planBuilder.setPlatformUdfPath("java", "/data/TestSmallWebCaseFunc.class");
            planBuilder.setPlatformUdfPath("spark", "/data/TestSmallWebCaseFunc.class");

            // 创建节点
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("input", "data/test.csv");
            }});

            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
                put("deterministic", "true");
            }});

            DataQuanta mapNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapFunc");
                put("deterministic", "true");
            }});

            DataQuanta reduceNode = DataQuanta.createInstance("reducebykey", new HashMap<String, String>() {{
                put("udfName", "reduceFunc");
                put("keyName", "reduceKey");
            }});

            DataQuanta sortNode = DataQuanta.createInstance("sort", new HashMap<String, String>() {{
                put("udfName", "sortFunc");
                put("deterministic", "true");
            }});

            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("output", "data/output.csv"); // 具体resources的路径通过配置文件获得
            }});

            // 链接节点，即构建DAG
            sourceNode.outgoing(mapNode, null);
            mapNode.outgoing(filterNode, null);
            filterNode.outgoing(reduceNode, null);
            reduceNode.outgoing(sortNode, null);
            sortNode.outgoing(sinkNode, null);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
