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

            DataQuanta sourceNode3 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/data/courseSelection.csv");
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
//                // 复杂聚合
//                put("sqlNeedForOptimized", "select sum(grade),avg(sgrade) from student,grade "
//                        + "where student.id=grade.id group by gender having sum(grade)>11 ");
                // 多重子查询与多表连接
                put("sqlNeedForOptimized", "select distinct student.id as stuNum,name as stuName,grade as stuGrade "
                        + "from student,courseSelection,grade "
                        + "where student.id=courseSelection.id and student.id=grade.id and cid in "
                        + "(select cid from courseSelection,student "
                        + "where courseSelection.id=student.id and student.id in "
                        + "(select id from student where student.name='xiaoming'))");
//                // 普通where子句（匹配字符串类型）
//                put("sqlNeedForOptimized", "select * from student where name='xiaoming' and gender='male'");
//                // 普通where子句（匹配数字类型）
//                put("sqlNeedForOptimized", "select * from student where year=2010");
            }});

            DataQuanta sinkNode = DataQuanta.createInstance("table-sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/docking/hdfs");
            }});

            planBuilder.addVertex(sourceNode1);
            planBuilder.addVertex(sourceNode2);
            planBuilder.addVertex(sourceNode3);
            planBuilder.addVertex(queryNode);
            planBuilder.addVertex(sinkNode);

            planBuilder.addEdge(sourceNode1, queryNode);
            planBuilder.addEdge(sourceNode2, queryNode);
            planBuilder.addEdge(sourceNode3, queryNode);
            planBuilder.addEdge(queryNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
