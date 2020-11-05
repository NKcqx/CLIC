package demo;

import api.DataQuanta;
import api.PlanBuilder;

import java.util.HashMap;

/**
 * Java平台，Stream与Table的转换
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/27 11:00 pm
 */
public class JavaConvertDemo {

    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();
            // 设置udf路径
            planBuilder.setPlatformUdfPath("java", "D:/2020project/convert/TestConvertFunc.class");
            //供测试生成文件使用
            planBuilder.setPlatformUdfPath("spark", "D:/2020project/convert/TestConvertFunc.class");

            DataQuanta sourceNode1 = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/convert/test.csv");
            }}).withTargetPlatform("java");

            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("java");

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

            // 在这里将原 Demo.java 的 workflow 与 java DataFrame 的 workflow 拼接
            // Stream -> DataFrame
            DataQuanta toTableNode = DataQuanta.createInstance("to-table", new HashMap<String, String>() {{
                put("udfName", "toTableFunc");
            }}).withTargetPlatform("java");

            DataQuanta sinkNode1 = DataQuanta.createInstance("table-sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/convert/javaTableOutput.csv");
            }}).withTargetPlatform("java");

            DataQuanta sourceNode2 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/convert/javaTableOutput.csv");
            }}).withTargetPlatform("java");

            // DataFrame -> Stream
            DataQuanta fromTableNode = DataQuanta.createInstance("from-table", null).withTargetPlatform("java");

            DataQuanta sinkNode2 = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/convert/javaOutput.csv"); // 具体resources的路径通过配置文件获得
            }}).withTargetPlatform("java");

            planBuilder.addVertex(sourceNode1);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapNode);
            planBuilder.addVertex(reduceNode);
            planBuilder.addVertex(sortNode);
            planBuilder.addVertex(toTableNode);
            planBuilder.addVertex(sinkNode1);
            planBuilder.addVertex(sourceNode2);
            planBuilder.addVertex(fromTableNode);
            planBuilder.addVertex(sinkNode2);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode1, filterNode);
            planBuilder.addEdge(filterNode, mapNode);
            planBuilder.addEdge(mapNode, reduceNode);
            planBuilder.addEdge(reduceNode, sortNode);
            planBuilder.addEdge(sortNode, toTableNode);
            planBuilder.addEdge(toTableNode, sinkNode1);
            planBuilder.addEdge(sinkNode1, sourceNode2);
            planBuilder.addEdge(sourceNode2, fromTableNode);
            planBuilder.addEdge(fromTableNode, sinkNode2);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
