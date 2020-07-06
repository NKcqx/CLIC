package platform.test;

import api.DataQuanta;
import api.PlanBuilder;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import platform.spark.SparkPlatform;

import java.util.HashMap;
import java.util.List;

public class SparkWorkflowTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkWorkflowTest.class);

    @Test
    public void sparkWorkflowTest() throws Exception {
        PlanBuilder planBuilder = new PlanBuilder();
        // 创建节点
        DataQuanta sourceNode1 = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("data_path", "customer.parquet");
        }});

        DataQuanta sourceNode2 = planBuilder.readDataFrom(new HashMap<String, String>() {{
            put("data_path", "orders.parquet");
        }});

        DataQuanta joinNode = DataQuanta.createInstance("join", new HashMap<String, String>() {{
            put("predicate", "$c_custkey==$o_custkey");
        }});   /// 根据ability 找到对应的operator所在的目录

        DataQuanta projectNode = DataQuanta.createInstance("project", new HashMap<String, String>() {{
            put("predicate", "o_totalprice");
        }});

        DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
            put("predicate", "o_totalprice > 100.0");
        }});

        DataQuanta collectNode = DataQuanta.createInstance("collect", null);

        // 链接节点，即构建DAG
        sourceNode1.outgoing(joinNode, null);
        sourceNode2.outgoing(joinNode, null);

        joinNode.outgoing(filterNode,
                null//new HashMap<String, String>(){{ put("sorted_value", "data"); }}
        );

        filterNode.outgoing(projectNode,
                null//new HashMap<String, String>(){{ put("sorted_value", "data"); }}
        );

        projectNode.outgoing(collectNode,
                null//new HashMap<String, String>(){{ put("sorted_value", "data"); }}
        );

        List<Row> object = (List<Row>) SparkPlatform.SparkRunner(planBuilder);
        int numRow = (int) (object.size() * 0.0001);
        for (int i = 0; i < numRow; i++) {
            LOGGER.info(object.get(i).toString());
        }

    }
}
