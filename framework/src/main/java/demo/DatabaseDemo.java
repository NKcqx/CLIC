package demo;

import api.DataQuanta;
import api.PlanBuilder;

import java.util.HashMap;

/**
 * 数据源是数据库，从数据库中读取数据
 * 本demo任务是读取三个table，执行三表join的sql
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/16 12:30 am
 */
public class DatabaseDemo {
    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();

            // 下面的udf声明在本demo实际没有用，只是为了防止ExecutableJavaOperator.java报下标越界错误
            planBuilder.setPlatformUdfPath("java", "/Users/jason/Desktop/TestSmallWebCaseFunc.class");
            planBuilder.setPlatformUdfPath("spark", "/Users/jason/Desktop/TestSmallWebCaseFunc.class");

            // 连接数据库
            DataQuanta connectNode = planBuilder.connectDB(new HashMap<String, String>() {{
                put("url", "jdbc:mysql://192.168.100.134:3306/clicdb?serverTimezone=Asia/Shanghai");
                put("driver", "com.mysql.cj.jdbc.Driver");
                put("user", "root");
                put("password", "123456");
//                put("url", "jdbc:postgresql://192.168.100.134:5432/clicdb");
//                put("driver", "org.postgresql.Driver");
//                put("user", "xiaohua");
//                put("password", "123456");
            }}).withTargetPlatform("spark");

            // 读取table
            DataQuanta dbSource1 = DataQuanta.createInstance("dbtable-source", new HashMap<String, String>() {{
                put("dbtable", "student");
            }}).withTargetPlatform("spark");

            DataQuanta dbSource2 = DataQuanta.createInstance("dbtable-source", new HashMap<String, String>() {{
                put("dbtable", "grade");
            }}).withTargetPlatform("spark");

            DataQuanta dbSource3 = DataQuanta.createInstance("dbtable-source", new HashMap<String, String>() {{
                put("dbtable", "course");
            }}).withTargetPlatform("spark");

            // 执行sql
            DataQuanta queryNode = DataQuanta.createInstance("query", new HashMap<String, String>() {{
                // 查找跟叫"xiaoming"的学生选同一门课的学生学号、姓名、成绩（"xiaoming"会重名并且和别人会有不止一门课一起上）
                put("sqlText", "select distinct student.id as stuNum,name as stuName,grade as stuGrade "
                        + "from student,course,grade "
                        + "where student.id=course.id and student.id=grade.id and cid in "
                        + "(select cid from course,student "
                        + "where course.id=student.id and student.id in "
                        + "(select id from student where student.name='xiaoming'))");
            }});
//
            // sink
            DataQuanta sinkNode = DataQuanta.createInstance("table-sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/db/hdfs");
            }});

            planBuilder.addVertex(connectNode);
            planBuilder.addVertex(dbSource1);
            planBuilder.addVertex(dbSource2);
            planBuilder.addVertex(dbSource3);
            planBuilder.addVertex(queryNode);
            planBuilder.addVertex(sinkNode);

            planBuilder.addEdge(connectNode, dbSource1);
            planBuilder.addEdge(connectNode, dbSource2);
            planBuilder.addEdge(connectNode, dbSource3);
            planBuilder.addEdge(dbSource1, queryNode);
            planBuilder.addEdge(dbSource2, queryNode);
            planBuilder.addEdge(dbSource3, queryNode);
            planBuilder.addEdge(queryNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}