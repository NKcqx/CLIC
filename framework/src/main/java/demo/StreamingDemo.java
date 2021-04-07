package demo;

import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author lfy
 * @version 1.0
 * @since 2021/4/6 3:30 pm
 */
public class StreamingDemo {

    public static void main(String[] args) throws Exception {
        try {
            PlanBuilder planBuilder = new PlanBuilder("test-streaming-case");
            // 这里不需要绝对路径，因为spark streaming的机制有所不同
            planBuilder.setPlatformUdfPath("java",
                    "TestStreamingFunc.class");
            planBuilder.setPlatformUdfPath("spark",
                    "TestStreamingFunc.class");

            // 创建节点
            DataQuanta sourceNode = planBuilder.readStreamFrom(new HashMap<String, String>() {{
                put("kind", "file");
                // 注意这里传入的是一个文件夹路径
                // 因为spark streaming读取文件时与flink不同，前者监测文件夹是否有数据流的变化，有则进行计算
                put("inputPath", "file:///D:/2020project/data/stream");
//                put("kind", "socket");
//                put("ip", "localhost");
//                put("port", "9999");
            }}).withTargetPlatform("spark");

            DataQuanta filterNode = DataQuanta.createInstance("stream-filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("spark");

            DataQuanta mapToPairNode = DataQuanta.createInstance("stream-map-to-pair", new HashMap<String, String>() {{
                put("keyName", "keyFunc");
                put("valueName", "valueFunc");
            }}).withTargetPlatform("spark");

            DataQuanta reduceByKeyNode = DataQuanta.createInstance("stream-reduce", new HashMap<String, String>() {{
            }}).withTargetPlatform("spark");

            DataQuanta windowNode = DataQuanta.createInstance("stream-window", new HashMap<String, String>() {{
                put("winLength", "9");
                put("rollLength", "3");
            }}).withTargetPlatform("spark");

            // 最终结果的输出路径
            DataQuanta sortNode = DataQuanta.createInstance("stream-sort", new HashMap<String, String>() {{
                put("topNum", "10");
            }}).withTargetPlatform("spark");

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapToPairNode);
            planBuilder.addVertex(reduceByKeyNode);
            planBuilder.addVertex(windowNode);
            planBuilder.addVertex(sortNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, filterNode);
            planBuilder.addEdge(filterNode, mapToPairNode);
            planBuilder.addEdge(mapToPairNode, reduceByKeyNode);
            planBuilder.addEdge(reduceByKeyNode, windowNode);
            planBuilder.addEdge(windowNode, sortNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

