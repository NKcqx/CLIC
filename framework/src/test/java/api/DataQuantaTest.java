package api;

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
            put("input", "fake path");
        }});
        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "udfNameValue");
            put("result", "resultVaule");
        }});
//        assert dataQuanta1.incoming(dataQuanta, new HashMap<String, String>() {{
//            put("incoming.output_key", "this.input_key");
//        }})==1;
        dataQuanta1.incoming(dataQuanta, "output_key", "input_key");
        assert dataQuanta1.getOperator().getInputChannel().get(0) == dataQuanta.getOperator().getOutputChannel().get(0);

    }

    @Test
    public void testOutgoing() throws Exception {
        //OperatorFactory.initMapping("framework/resources/OperatorTemplates/OperatorMapping.xml");
        PlanBuilder planBuilder = new PlanBuilder();
        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("input", "fake path");
        }});
        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "udfNameValue");
            put("result", "resultVaule");
        }});

        dataQuanta.outgoing(dataQuanta1,"output_key", "input_key");
        assert dataQuanta.getOperator().getOutputChannel().get(0) == dataQuanta1.getOperator().getInputChannel().get(0);
    }


}
