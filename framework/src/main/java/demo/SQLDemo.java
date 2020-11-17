package demo;

import api.DataQuanta;
import api.PlanBuilder;

import java.util.HashMap;

/**
 * SparkSQL
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/27 11:00 pm
 */
public class SQLDemo {

    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();

            /**
             * 在这里，用户可以选取多个源文件（txt, csv, json等）
             * 源文件可以是本地也可以是HDFS上分布式存储的文件
             * 来满足连接、嵌套查询等sql操作
             */
            DataQuanta sourceNode1 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/sql/student.csv");
            }}).withTargetPlatform("spark");

            DataQuanta sourceNode2 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/sql/grade.csv");
            }}).withTargetPlatform("spark");

            DataQuanta sourceNode3 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/sql/courseSelection.csv");
            }}).withTargetPlatform("spark");

            DataQuanta queryNode = DataQuanta.createInstance("query", new HashMap<String, String>() {{
                // 查找跟叫"xiaoming"的学生选同一门课的学生学号、姓名、成绩（"xiaoming"会重名并且和别人会有不止一门课一起上）
                put("sqlText", "select distinct student.id as stuNum,name as stuName,grade as stuGrade "
                        + "from student,courseSelection,grade "
                        + "where student.id=courseSelection.id and student.id=grade.id and cid in "
                        + "(select cid from courseSelection,student "
                        + "where courseSelection.id=student.id and student.id in "
                        + "(select id from student where student.name='xiaoming'))");
            }}).withTargetPlatform("spark");

            // 最终结果的输出路径 这里写文件夹名字（HDFS形式）
            DataQuanta sinkNode = DataQuanta.createInstance("table-sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/sql/hdfs");
            }});

            planBuilder.addVertex(sourceNode1);
            planBuilder.addVertex(sourceNode2);
            planBuilder.addVertex(sourceNode3);
            planBuilder.addVertex(queryNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
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
