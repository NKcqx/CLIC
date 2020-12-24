package demo;

import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/12/15 20:31
 */
public class GraphDemo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            PlanBuilder planBuilder = new PlanBuilder("test-graph");
            planBuilder.setPlatformUdfPath("java", "/data/udfs/TestGraphCaseFunc.class");
            planBuilder.setPlatformUdfPath("spark", "/data/udfs/TestGraphCaseFunc.class");
            planBuilder.setPlatformUdfPath("graphchi", "/data/udfs/TestGraphCaseFunc.class");

            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "/data/datasets/graph_test_dqh/0.csv");
                put("separator", ",");
                put("partitionNum","2");
            }}).withTargetPlatform("spark");
            //在使用时，注意设置默认Separate为空格，注意graphchi的edglist格式要求

            DataQuanta toGraphNode = DataQuanta.createInstance("convert-to-graph", new HashMap<String, String>()
            ).withTargetPlatform("spark");
            DataQuanta pageRankNode = DataQuanta.createInstance("pagerank", new HashMap<String, String>() {{
                put("iterNum", "1000");
//                put("shardNum", "2");
                //graphchi:For specifying the number of shards, 20-50 million edges/shard is often a good configuration.(20，000，000/shard)
            }}).withTargetPlatform("spark");

            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("outputPath", "/data/datasets/output.csv"); // 具体resources的路径通过配置文件获得
            }}).withTargetPlatform("spark");

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(toGraphNode);
            planBuilder.addVertex(pageRankNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
//            planBuilder.addEdge(sourceNode, pageRankNode);
            planBuilder.addEdge(sourceNode, toGraphNode);
            planBuilder.addEdge(toGraphNode, pageRankNode);
            planBuilder.addEdge(pageRankNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
