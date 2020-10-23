package sql;

import api.DataQuanta;
import api.PlanBuilder;
import org.javatuples.Pair;

import java.util.HashMap;

/**
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/7 6:30 下午
 */
public class SQLDemo {

    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();

            /**
             * 在这里，用户可以选取多个源文件（txt, csv, json, parquet等）
             * 来满足连接、嵌套查询等sql操作
             */
            DataQuanta sourceNode1 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/sql/student.csv");
            }});

            DataQuanta sourceNode2 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/sql/grade.csv");
            }});

            DataQuanta sourceNode3 = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/sql/courseSelection.csv");
            }});

            DataQuanta exeNode = DataQuanta.createInstance("sqlExe", new HashMap<String, String>() {{
                put("tableNames", "student,grade,courseSelection");
//                // 查找成绩在8分以上的学生的id和姓名
//                put("sqlText", "select id,name from student where id in " +
//                        "(select student.id from student join grade on student.id=grade.id and grade.grade>=8)");
                // 查找跟叫"xiaoming"的学生选同一门课的学生学号和姓名（"xiaoming"会重名并且和别人会有不止一门课一起上）
                put("sqlText", "select distinct student.id as stuNum,name as stuName " +
                        "from student,courseSelection " +
                        "where student.id=courseSelection.id and cid in " +
                        "(select cid from courseSelection,student where courseSelection.id=student.id and student.id in " +
                        "(select id from student where student.name='xiaoming'))");
            }});

            // 最终结果的输出路径 这里写文件夹名字（HDFS形式）
            DataQuanta sinkNode = DataQuanta.createInstance("sqlSink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/sql/hdfs");
            }});

            planBuilder.addVertex(sourceNode1);
            planBuilder.addVertex(sourceNode2);
            planBuilder.addVertex(sourceNode3);
            planBuilder.addVertex(exeNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode1, exeNode, new Pair<>("result", "table1"));
            planBuilder.addEdge(sourceNode2, exeNode, new Pair<>("result", "table2"));
            planBuilder.addEdge(sourceNode3, exeNode, new Pair<>("result", "table3"));
            planBuilder.addEdge(exeNode, sinkNode, null);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
