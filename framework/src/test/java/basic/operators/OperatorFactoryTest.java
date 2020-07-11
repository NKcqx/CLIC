package basic.operators;

import basic.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Testing for OperatorFactory.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/11 11:53
 */
public class OperatorFactoryTest {

    @Before
    public void before() {
        /**
         * Solve the contradiction between junit and System.getProperty("user.dir")
         */
        String userDir = "user.dir";
        // 下面路径根据本地实际情况改，只要到项目根目录就行
        System.setProperty(userDir, "D:\\IRDemo\\");
    }

    @SuppressWarnings("checkstyle:LeftCurly")
    @Test
    public void initMappingTest() throws Exception {
        Configuration configuration = new Configuration();
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));

        // 用spring-test包的ReflectionTestUtils从外部访问私有静态成员
        Map<String, String> factoryMapping =
                (Map<String, String>) ReflectionTestUtils.getField(OperatorFactory.class, "mapping");
        for (Map.Entry<String, String> entry : factoryMapping.entrySet()) {
            if (entry.getKey().equals("filter")) {
                assertEquals("/framework/resources/Operator/Filter/conf/FilterOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("reducebykey"))
            {
                assertEquals("/framework/resources/Operator/ReduceByKey/conf/ReduceByKeyOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("sink")) {
                assertEquals("/framework/resources/Operator/Sink/conf/SinkOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("project")) {
                assertEquals("/framework/resources/Operator/Project/conf/ProjectOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("source")) {
                assertEquals("/framework/resources/Operator/Source/conf/SourceOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("sort")) {
                assertEquals("/framework/resources/Operator/Sort/conf/SortOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("join")) {
                assertEquals("/framework/resources/Operator/Join/conf/JoinOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("map")) {
                assertEquals("/framework/resources/Operator/Map/conf/MapOperator.xml",
                        entry.getValue());
            }
            if (entry.getKey().equals("collect")) {
                assertEquals("/framework/resources/Operator/Collect/conf/CollectOperator.xml",
                        entry.getValue());
            }
        }
    }

    @Test
    public void getTemplateTest() throws Exception {
        Configuration configuration = new Configuration();
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));

        // 通过java反射访问私有静态方法
        Method getTemplateMethod = Class.forName("basic.operators.OperatorFactory")
                .getDeclaredMethod("getTemplate", new Class[]{String.class});
        getTemplateMethod.setAccessible(true);

        String template;

        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "filter");
        assertEquals(template, "/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "reducebykey");
        assertEquals(template, "/framework/resources/Operator/ReduceByKey/conf/ReduceByKeyOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "sink");
        assertEquals(template, "/framework/resources/Operator/Sink/conf/SinkOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "project");
        assertEquals(template, "/framework/resources/Operator/Project/conf/ProjectOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "source");
        assertEquals(template, "/framework/resources/Operator/Source/conf/SourceOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "sort");
        assertEquals(template, "/framework/resources/Operator/Sort/conf/SortOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "join");
        assertEquals(template, "/framework/resources/Operator/Join/conf/JoinOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "map");
        assertEquals(template, "/framework/resources/Operator/Map/conf/MapOperator.xml");
        template = (String) getTemplateMethod.invoke(OperatorFactory.class, "collect");
        assertEquals(template, "/framework/resources/Operator/Collect/conf/CollectOperator.xml");
    }

    @Test
    public void createOperatorTest() throws Exception {
        Configuration configuration = new Configuration();
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));

        Operator opt = OperatorFactory.createOperator("filter");
        Operator spyOpt = PowerMockito.spy(opt);

        String configFilePath =
                Whitebox.getInternalState(spyOpt, "configFilePath");
        assertEquals("/framework/resources/Operator/Filter/conf/FilterOperator.xml", configFilePath);
    }
}
