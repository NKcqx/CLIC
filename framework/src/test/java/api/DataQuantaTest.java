package api;

import basic.operators.OperatorFactory;
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

        int end=System.getProperty("user.dir").indexOf("framework")-1;
        System.setProperty("user.dir",System.getProperty("user.dir").substring(0,end));
        System.out.println(System.getProperty("user.dir"));

        OperatorFactory.initMapping("framework/resources/OperatorTemplates/OperatorMapping.xml");
        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("input", "fake path");
        }});
        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "udfNameValue");
            put("result", "resultVaule");
        }});
        assert dataQuanta1.incoming(dataQuanta, new HashMap<String, String>() {{
            put("incoming.output_key", "this.input_key");
        }})==1;
    }

    @Test
    public void testOutgoing() throws Exception {

        int end=System.getProperty("user.dir").indexOf("framework")-1;
        System.setProperty("user.dir",System.getProperty("user.dir").substring(0,end));
        System.out.println(System.getProperty("user.dir"));

        OperatorFactory.initMapping("framework/resources/OperatorTemplates/OperatorMapping.xml");
        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("input", "fake path");
        }});
        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
            put("udfName", "udfNameValue");
            put("result", "resultVaule");
        }});
        assert dataQuanta1.outgoing(dataQuanta, new HashMap<String, String>() {{
            put("incoming.output_key", "this.input_key");
        }})==1;
    }


}
