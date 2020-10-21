package sql;

import api.DataQuanta;
import api.PlanBuilder;
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

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            /**
             * 在这里，用户可以选取多个源文件（txt, csv, json, parquet等）
             * 来满足连接、嵌套查询等sql操作
             */
            DataQuanta sourceNode = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath1", "D:/2020project/sql/student.csv");
                put("tableName1", "student");
                put("inputPath2", "D:/2020project/sql/grade.csv");
                put("tableName2", "grade");
                put("inputPath3", "D:/2020project/sql/courseSelection.csv");
                put("tableName3", "courseSelection");
//                // 查找成绩在8分以上的学生的id和姓名
//                put("sqlText", "select id,name from student where id in " +
//                        "(select student.id from student join grade on student.id=grade.id and grade.grade>=8)");
                // 查找跟叫"xiaoming"的学生选同一门课的学生学号和姓名（"xiaoming"会重名并且和别人会有不止一门课一起上）
                put("sqlText", "select distinct student.id as stuNum, name as stuName from student,courseSelection " +
                        "where student.id=courseSelection.id and cid in " +
                        "(select cid from courseSelection,student where courseSelection.id=student.id and student.id in " +
                        "(select id from student where student.name='xiaoming'))");
            }});

            // 最终结果的输出路径 这里写文件夹名字（HDFS形式）
            DataQuanta sinkNode = DataQuanta.createInstance("sqlSink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/sql/hdfs");
            }});

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, sinkNode, null);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
