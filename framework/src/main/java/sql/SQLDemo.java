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
            DataQuanta sourceNode = planBuilder.readTableFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/sqlTest.txt");
            }});

            DataQuanta sqlExeNode = DataQuanta.createInstance("sqlExe", new HashMap<String, String>() {{
                put("sqlText", "select * from person");
            }});

            // 最终结果的输出路径   例如该map的value值是本项目output.csv的绝对路径
            DataQuanta sinkNode = DataQuanta.createInstance("sqlSink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/sqlOutput.csv"); // 具体resources的路径通过配置文件获得
            }});

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(sqlExeNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, sqlExeNode, null);
            planBuilder.addEdge(sqlExeNode, sinkNode, null);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
