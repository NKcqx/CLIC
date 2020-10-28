package api;

import basic.Configuration;
import basic.operators.OperatorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.NoSuchElementException;


/**
 * @Author nathan, 陈齐翔
 * @Date 2020/7/8 1:23 下午
 * @Version 1.0
 */
public class DataQuantaTest {
    public DataQuanta dataQuanta1;
    public DataQuanta dataQuanta2;
    public Configuration configuration;
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Before
    public void before() throws Exception {
        configuration = new Configuration();
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
        dataQuanta1 = DataQuanta.createInstance("source", new HashMap<String, String>() {{
            put("inputPath", "fake path");
        }});
        dataQuanta2 = DataQuanta.createInstance("empty", new HashMap<String, String>() {{
            put("inputPath", "fake path");
        }});
    }

    @Test
    public void testCreateInstance() {
        assertNotNull(dataQuanta1);
        assertNull(dataQuanta2);
    }

    @Test
    public void testEquals(){
        assertFalse(dataQuanta1.equals(dataQuanta2));
        assertTrue(dataQuanta1.equals(dataQuanta1));
    }
//  下面这部分应该是PlanBuilder的测试
//    @Test
//    public void testIncoming() throws Exception {
//
////        OperatorFactory.initMapping("framework/resources/OperatorTemplates/OperatorMapping.xml");
//        PlanBuilder planBuilder = new PlanBuilder();
//        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
//            put("inputPath", "fake path");
//        }});
//        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
//            put("udfName", "udfNameValue");
//        }});
////        assert dataQuanta1.incoming(dataQuanta, new HashMap<String, String>() {{
////            put("incoming.output_key", "this.input_key");
////        }})==1;
//        planBuilder.addVertex(dataQuanta);
//        planBuilder.addVertex(dataQuanta1);
//        planBuilder.addEdge(dataQuanta, dataQuanta1, new Pair<>("input_key", "output_key"));
//        // 有向图，上面只构建了 dq -> dq1的边
//        Assert.assertNull(planBuilder.getGraph().getEdge(dataQuanta1.getOperator(), dataQuanta.getOperator()));
//        Assert.assertNotNull(planBuilder.getGraph().getEdge(dataQuanta.getOperator(), dataQuanta1.getOperator()));
//       // assert dataQuanta1.getOperator().getInputChannel().get(0) == dataQuanta.getOperator().getOutputChannel().get(0);
//    }
//
//    @Test
//    public void testOutgoing() throws Exception {
//        //OperatorFactory.initMapping("framework/resources/OperatorTemplates/OperatorMapping.xml");
//        PlanBuilder planBuilder = new PlanBuilder();
//        DataQuanta dataQuanta = DataQuanta.createInstance("source", new HashMap<String, String>() {{
//            put("inputPath", "fake path");
//        }});
//        DataQuanta dataQuanta1 = DataQuanta.createInstance("map", new HashMap<String, String>() {{
//            put("udfName", "udfNameValue");
//        }});
//        planBuilder.addVertex(dataQuanta);
//        planBuilder.addVertex(dataQuanta1);
//        planBuilder.addEdge(dataQuanta1, dataQuanta,  new Pair<>("output_key", "input_key"));
//        Assert.assertNull(planBuilder.getGraph().getEdge(dataQuanta.getOperator(), dataQuanta1.getOperator()));
//        Assert.assertNotNull(planBuilder.getGraph().getEdge(dataQuanta1.getOperator(), dataQuanta.getOperator()));
//    }

    @Test
    public void testWithTargetPlatform(){
        try {
            DataQuanta dq1 = DataQuanta.createInstance("filter", null).withTargetPlatform("java");
            Assert.assertNotNull(dq1.getOperator().getSelectedEntities());
            Assert.assertEquals(dq1.getOperator().getSelectedEntities().getEntityID(), "java");

            DataQuanta dq2 = DataQuanta.createInstance("filter", null).withTargetPlatform("spark");
            Assert.assertNotNull(dq2.getOperator().getSelectedEntities());
            Assert.assertEquals(dq2.getOperator().getSelectedEntities().getEntityID(), "spark");
            dq2.withTargetPlatform("java");
            Assert.assertEquals(dq2.getOperator().getSelectedEntities().getEntityID(), "java");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testThrowExceptionWhenSelectPlatform(){
        try {
            DataQuanta dq3 = DataQuanta.createInstance("filter", null);
            assertThrows(NoSuchElementException.class, ()->dq3.withTargetPlatform("invalid-platform"));
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
