package demo;

import api.DataQuanta;
import api.PlanBuilder;

import java.util.HashMap;

/**
 * 与Siamese组的项目对接demo
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/18 9:00 pm
 */
public class DockingDemo {

    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();

            DataQuanta sourceNode1 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/data/student.csv");
            }}).withTargetPlatform("spark");

            DataQuanta sourceNode2 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/data/grade.csv");
            }}).withTargetPlatform("spark");

            /**
             * 这里是跟Siamese组对接的算子
             */
            DataQuanta queryNode = DataQuanta.createInstance("query", new HashMap<String, String>() {{
//                // 连接
//                put("sqlNeedForOptimized", "select student.id,name,grade.grade from student,grade "
//                        + "where student.id=grade.id and grade>2");
//                // 自定义列名
//                put("sqlNeedForOptimized", "select student.id as stuid,name as stuname from student,grade "
//                         + "where student.id = grade.id");
//                // 聚合
//                put("sqlNeedForOptimized", "select sum(grade) from student,grade "
//                        + "where student.id=grade.id group by gender");
                // 复杂聚合
                put("sqlNeedForOptimized", "select sum(grade),avg(sgrade) from student,grade "
                        + "where student.id=grade.id group by gender having sum(grade)>11 ");
            }});

            DataQuanta sinkNode = DataQuanta.createInstance("table-sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/docking/hdfs");
            }});

            planBuilder.addVertex(sourceNode1);
            planBuilder.addVertex(sourceNode2);
            planBuilder.addVertex(queryNode);
            planBuilder.addVertex(sinkNode);

            planBuilder.addEdge(sourceNode1, queryNode);
            planBuilder.addEdge(sourceNode2, queryNode);
            planBuilder.addEdge(queryNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
