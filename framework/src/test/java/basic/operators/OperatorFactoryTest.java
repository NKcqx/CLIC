package basic.operators;

import basic.Configuration;
import org.junit.Before;
import org.junit.Test;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

/**
 * Testing for OperatorFactory.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/11 11:53
 */
public class OperatorFactoryTest {

    private Configuration configuration;

    @Before
    public void before() throws Exception {
        configuration = new Configuration();
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
    }

//    @Before
//    public void initMappingTest() throws Exception {
//        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
//    }

    @Test
    public void getTemplateTest() throws Exception {
//        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));

        // 通过java反射访问私有静态方法
        Method getTemplateMethod = Class.forName("basic.operators.OperatorFactory")
                .getDeclaredMethod("getTemplate", new Class[]{String.class});
        getTemplateMethod.setAccessible(true);

        String template;

        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "filter");
        assertEquals(template, "Operator/Filter/conf/FilterOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "reduce-by-key");
        assertEquals(template, "Operator/ReduceByKey/conf/ReduceByKeyOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "sink");
        assertEquals(template, "Operator/Sink/conf/SinkOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "project");
        assertEquals(template, "Operator/Project/conf/ProjectOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "source");
        assertEquals(template, "Operator/Source/conf/SourceOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "sort");
        assertEquals(template, "Operator/Sort/conf/SortOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "join");
        assertEquals(template, "Operator/Join/conf/JoinOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "map");
        assertEquals(template, "Operator/Map/conf/MapOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "collect");
        assertEquals(template, "Operator/Collect/conf/CollectOperator.xml");
    }

    @Test
    public void createOperatorTest() throws Exception {
//        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));

        Operator opt = OperatorFactory.createOperator("filter");
        Operator spyOpt = spy(opt);

        assertEquals("FilterOperator", spyOpt.getOperatorName());
    }
}
