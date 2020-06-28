package platform.test;

import api.DataQuanta;
import api.PlanBuilder;
import org.apache.spark.sql.Row;
import org.junit.Test;
import platform.spark.SparkPlatform;

import java.util.HashMap;
import java.util.List;

public class SparkWorkflowTest {

    @Test
    public void sparkWorkflowTest() throws Exception {
        PlanBuilder planBuilder = new PlanBuilder();
        // 创建节点
        DataQuanta source_node1 = planBuilder.readDataFrom(new HashMap<String, String>(){{
            put("data_path", "customer.parquet");
        }});

        DataQuanta source_node2 = planBuilder.readDataFrom(new HashMap<String, String>(){{
            put("data_path", "orders.parquet");
        }});


        DataQuanta join_node = DataQuanta.createInstance("join", new HashMap<String,String>(){{
            put("predicate","$c_custkey==$o_custkey");
        }});   /// 根据ability 找到对应的operator所在的目录

        DataQuanta project_node = DataQuanta.createInstance("project", new HashMap<String, String>(){{
            put("predicate", "o_totalprice");
        }});

        DataQuanta filter_node = DataQuanta.createInstance("filter", new HashMap<String, String>(){{
            put("predicate", "o_totalprice > 100.0");
        }});

        DataQuanta collect_node = DataQuanta.createInstance("collect", null);

        // 链接节点，即构建DAG
        source_node1.outgoing(join_node, null);
        source_node2.outgoing(join_node,null);

        join_node.outgoing(filter_node,
                null//new HashMap<String, String>(){{ put("sorted_value", "data"); }}
        );


        filter_node.outgoing(project_node,
                null//new HashMap<String, String>(){{ put("sorted_value", "data"); }}
            );

        project_node.outgoing(collect_node,
                null//new HashMap<String, String>(){{ put("sorted_value", "data"); }}
        );


         List<Row> object=(List<Row>) SparkPlatform.SparkRunner(planBuilder);
         int numRow= (int) (object.size()*0.0001);
         for(int i=0;i<numRow;i++){
             System.out.println(object.get(i));
         }

    }
}
