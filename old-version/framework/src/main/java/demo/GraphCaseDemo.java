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
 * @since 2020/12/01 15:27
 */
public class GraphCaseDemo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            PlanBuilder planBuilder = new PlanBuilder("test-graph-case");
            // 设置udf路径   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("java", "/data/udfs/TestGraphCaseFunc.class");
            //供测试生成文件使用   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("spark", "/data/udfs/TestGraphCaseFunc.class");
            planBuilder.setPlatformUdfPath("graphchi", "/data/udfs/TestGraphCaseFunc.class");
            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "/data/datasets/dqh_graph_test.csv");
                put("separator", ",");
            }}).withTargetPlatform("spark");
            DataQuanta mapNode1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapDidFunc");
            }}).withTargetPlatform("spark");
//            DataQuanta sortNode1 = DataQuanta.createInstance("sort", new HashMap<String, String>() {{
//                put("udfName", "sortCountFunc");
//            }}).withTargetPlatform("spark");
            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("spark");
            DataQuanta mapNode2 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapCodeFunc");
            }}).withTargetPlatform("java");
            //在使用时，注意设置默认Separate为空格，注意graphchi的edglist格式要求

//            DataQuanta toGraphNode = DataQuanta.createInstance("convert-to-graph", new HashMap<String, String>()
//            ).withTargetPlatform("spark");
            DataQuanta pageRankNode = DataQuanta.createInstance("pagerank", new HashMap<String, String>() {{
                put("iterNum", "100");
//                put("graphName", "zhihu-fellowee");
                put("shardNum", "1");
            }}).withTargetPlatform("graphchi");
            DataQuanta mapNode3 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapMulFunc");
            }}).withTargetPlatform("java");
            DataQuanta sortNode = DataQuanta.createInstance("sort", new HashMap<String, String>() {{
                put("udfName", "sortFunc");
            }}).withTargetPlatform("java");
            // 最终结果的输出路径   例如该map的value值是本项目output.csv的绝对路径
            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("outputPath", "/data/datasets/output.csv"); // 具体resources的路径通过配置文件获得
            }}).withTargetPlatform("java");
            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(mapNode1);
//            planBuilder.addVertex(sortNode1);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapNode2);
//            planBuilder.addVertex(toGraphNode);
            planBuilder.addVertex(pageRankNode);
            planBuilder.addVertex(mapNode3);
            planBuilder.addVertex(sortNode);
            planBuilder.addVertex(sinkNode);
            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, mapNode1);
//            planBuilder.addEdge(mapNode1, sortNode1);
//            planBuilder.addEdge(sortNode1, filterNode);
            planBuilder.addEdge(mapNode1, filterNode);
            planBuilder.addEdge(filterNode, mapNode2);
//            planBuilder.addEdge(mapNode2, toGraphNode);
//            planBuilder.addEdge(toGraphNode, pageRankNode);
            planBuilder.addEdge(mapNode2, pageRankNode);
            planBuilder.addEdge(pageRankNode, mapNode3);
            planBuilder.addEdge(mapNode3, sortNode);
            planBuilder.addEdge(sortNode, sinkNode);
            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
