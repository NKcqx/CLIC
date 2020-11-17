package demo;

import api.DataQuanta;
import api.PlanBuilder;

import java.util.HashMap;

/**
 * Spark平台，RDD与Table的转换
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/27 11:00 pm
 */
public class ConvertDemo {

    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();
            // 设置udf路径
            planBuilder.setPlatformUdfPath("java", "D:/2020project/convert/TestConvertFunc.class");
            //供测试生成文件使用
            planBuilder.setPlatformUdfPath("spark", "D:/2020project/convert/TestConvertFunc.class");

            DataQuanta sourceNode1 = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/convert/test.csv");
            }}).withTargetPlatform("spark");

            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("spark");

            DataQuanta mapNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapFunc");
            }}).withTargetPlatform("spark");

            DataQuanta reduceNode = DataQuanta.createInstance("reduce-by-key", new HashMap<String, String>() {{
                put("udfName", "reduceFunc");
                put("keyName", "reduceKey");
            }}).withTargetPlatform("spark");

            DataQuanta sortNode = DataQuanta.createInstance("sort", new HashMap<String, String>() {{
                put("udfName", "sortFunc");
            }}).withTargetPlatform("spark");

            // 在这里将原 Demo.java 的 workflow 与 SQL 的 workflow 拼接
            // JavaRDD -> Dataset
            DataQuanta toTableNode = DataQuanta.createInstance("to-table", new HashMap<String, String>() {{
                put("udfName", "toTableFunc");
                put("tableName", "clickResult");
            }}).withTargetPlatform("spark");

            DataQuanta queryNode = DataQuanta.createInstance("query", new HashMap<String, String>() {{
                put("sqlText", "select url,clickTimes from clickResult where clickTimes >= 1");
            }}).withTargetPlatform("spark");

            // 以分布式形式存储table
            DataQuanta sinkNode1 = DataQuanta.createInstance("table-sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/convert/hdfs");
            }}).withTargetPlatform("spark");

            // 以分布式形式读取table
            DataQuanta sourceNode2 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/convert/hdfs");
            }}).withTargetPlatform("spark");

            // Dataset -> JavaRDD
            DataQuanta fromTableNode = DataQuanta.createInstance("from-table", null).withTargetPlatform("spark");

            DataQuanta sinkNode2 = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/convert/output.csv"); // 具体resources的路径通过配置文件获得
            }}).withTargetPlatform("spark");

            planBuilder.addVertex(sourceNode1);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapNode);
            planBuilder.addVertex(reduceNode);
            planBuilder.addVertex(sortNode);
            planBuilder.addVertex(toTableNode);
            planBuilder.addVertex(queryNode);
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
            planBuilder.addEdge(toTableNode, queryNode);
            planBuilder.addEdge(queryNode, sinkNode1);
            planBuilder.addEdge(sinkNode1, sourceNode2);
            planBuilder.addEdge(sourceNode2, fromTableNode);
            planBuilder.addEdge(fromTableNode, sinkNode2);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
