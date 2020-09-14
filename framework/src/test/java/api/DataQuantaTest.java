package api;

import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;


/**
 * @Author nathan
 * @Date 2020/7/8 1:23 下午
 * @Version 1.0
 */
public class DataQuantaTest {
    @Test
    public void testIncoming() throws Exception {

//        OperatorFactory.initMapping("framework/resources/OperatorTemplates/OperatorMapping.xml");
        PlanBuilder planBuilder = new PlanBuilder();
        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("inputPath", "fake path");
        }});
        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "udfNameValue");
        }});
//        assert dataQuanta1.incoming(dataQuanta, new HashMap<String, String>() {{
//            put("incoming.output_key", "this.input_key");
//        }})==1;
        planBuilder.addVertex(dataQuanta);
        planBuilder.addVertex(dataQuanta1);
        planBuilder.addEdge(dataQuanta, dataQuanta1, new Pair<>("input_key", "output_key"));
        // 有向图，上面只构建了 dq -> dq1的边
        Assert.assertNull(planBuilder.getGraph().getEdge(dataQuanta1.getOperator(), dataQuanta.getOperator()));
        Assert.assertNotNull(planBuilder.getGraph().getEdge(dataQuanta.getOperator(), dataQuanta1.getOperator()));
       // assert dataQuanta1.getOperator().getInputChannel().get(0) == dataQuanta.getOperator().getOutputChannel().get(0);

    }

    @Test
    public void testOutgoing() throws Exception {
        //OperatorFactory.initMapping("framework/resources/OperatorTemplates/OperatorMapping.xml");
        PlanBuilder planBuilder = new PlanBuilder();
        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("inputPath", "fake path");
        }});
        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "udfNameValue");
        }});
        planBuilder.addVertex(dataQuanta);
        planBuilder.addVertex(dataQuanta1);
        planBuilder.addEdge(dataQuanta1, dataQuanta,  new Pair<>("output_key", "input_key"));
        Assert.assertNull(planBuilder.getGraph().getEdge(dataQuanta.getOperator(), dataQuanta1.getOperator()));
        Assert.assertNotNull(planBuilder.getGraph().getEdge(dataQuanta1.getOperator(), dataQuanta.getOperator()));
    }


}
