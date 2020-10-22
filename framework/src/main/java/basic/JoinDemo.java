package basic;

import api.DataQuanta;
import api.PlanBuilder;

import java.util.HashMap;

/**
 * @author
 * @version 1.0
 * @since 2020/10/22 7:35 下午
 */
public class JoinDemo {
    public static void main(String[] args) {
        try {
            PlanBuilder planBuilder = new PlanBuilder();
            // 设置udf路径   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("java", "D:/2020project/join/TestJoinCaseFunc.class");
            //供测试生成文件使用   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("spark", "D:/2020project/join/TestJoinCaseFunc.class");

            // 第一条路
            DataQuanta sourceNode1 = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/join/companyInfo.csv");
            }});

            DataQuanta filterNode1 = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterCompanyInfoFunc");
            }});

            // 第二条路
            DataQuanta sourceNode2 = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "D:/2020project/webCompany.csv");
            }});
            DataQuanta filterNode2 = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterWebCompanyFunc");
            }});

            // join
            DataQuanta joinNode = DataQuanta.createInstance("join", new HashMap<String, String>() {{
                put("leftTableKeyName", "leftTableKey");
                put("rightTableKeyName", "rightTableKey");
                put("leftTableFuncName", "leftTableFunc");
                put("rightTableFuncName", "rightTableFunc");
            }});

            // 最终结果的输出路径   例如该map的value值是本项目output.csv的绝对路径
            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
                put("outputPath", "D:/2020project/join/output.csv"); // 具体resources的路径通过配置文件获得
            }});

            planBuilder.addVertex(sourceNode1);
            planBuilder.addVertex(filterNode1);
            planBuilder.addVertex(sourceNode2);
            planBuilder.addVertex(filterNode2);
            planBuilder.addVertex(joinNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode1, filterNode1);
            planBuilder.addEdge(filterNode1, joinNode);
            planBuilder.addEdge(sourceNode2, filterNode2);
            planBuilder.addEdge(filterNode2, joinNode);
            planBuilder.addEdge(joinNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
