package basic.operators;

import basic.Param;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Testing for Operator.java
 *
 * @author 刘丰艺, 陈齐翔
 * @version 1.0
 * @since 2020/7/10 0:52
 */
public class OperatorTest {

    private String configFilePath;
    private Operator spyOpt;
    private Operator sortOpt;
    private Operator opt;

    @Before
    public void before() throws Exception {
        configFilePath = "Operator/Filter/conf/FilterOperator.xml";
        opt = OperatorFactory.createOperatorFromFile(configFilePath);
        sortOpt = OperatorFactory.createOperatorFromFile("Operator/Sort/conf/SortOperator.xml");
        spyOpt = spy(OperatorFactory.createOperatorFromFile(configFilePath));
        spyOpt.setParamValue("udfName", "a name");
    }

    @Test
    public void simpleConstructorTest() throws Exception {
        Operator opt = OperatorFactory.createOperatorFromFile("Operator/Filter/conf/FilterOperator.xml");
    }

    @Test
    public void inputDataListTest() {
        // TODO: 如果将 operator 配置文件中的 parameters 标签放到具体平台的配置文件中，该测试不通过，需要修改
        String name = "udfName2";
        String dataType = "string";
        String defaultValue = null;
        Boolean isRequired = false;
        Param param = new Param(name, dataType, isRequired, defaultValue);
        Map<String, Param> inputDataList = spyOpt.getInputParamList();
        inputDataList.put(name, param);

        String[] expectedKeys = {"udfName", "udfName2"};
        String[] actualKeys = new String[2];

        int i = 0;
        for (Map.Entry<String, Param> entry : spyOpt.getInputParamList().entrySet()) {
            actualKeys[i] = entry.getKey();
            i += 1;
        }

        assertArrayEquals(expectedKeys, actualKeys);
    }

    @Test
    public void getPlatformOptConfTest() throws Exception {
        assert (spyOpt.getEntities().containsKey("java"));
        assert (spyOpt.getEntities().containsKey("spark"));
    }

    @Test
    public void selectEntityTest() throws Exception {
        spyOpt.selectEntity("java");
        verify(spyOpt, times(1)).selectEntity("java");
        verify(spyOpt, times(0)).selectEntity("spark");
    }

    @Test
    public void evaluateTest() {
        boolean flag = spyOpt.evaluate();
        assertEquals(true, flag);
        Mockito.when(spyOpt.evaluate()).thenReturn(false);
        boolean mockFlag = spyOpt.evaluate();
        assertEquals(false, mockFlag);
        verify(spyOpt, times(3)).evaluate();
    }


    @Test
    public void inputDataTest() {
        Param param = new Param("inputKey", "string", false, null);
        spyOpt.addInputData(param);
        param = new Param("inputKey2", "string", true, null);
        spyOpt.addInputData(param);

        List<String> expectedInputKeys = new ArrayList<>(Arrays.asList("data", "inputKey", "inputKey2"));
        for (Map.Entry<String, Param> entry : spyOpt.getInputDataList().entrySet()) {
            assertTrue(expectedInputKeys.contains(entry.getValue().getName()));
        }
    }

    @Test
    public void outputDataTest() {
        Param param = new Param("result1", "string", false, null);
        spyOpt.addOutputData(param);
        param = new Param("result2", "string", true, null);
        spyOpt.addOutputData(param);

        List<String> expectedOutputKeys = new ArrayList<>(Arrays.asList("result", "result1", "result2"));
        for (Map.Entry<String, Param> entry : spyOpt.getOutputDataList().entrySet()) {
            assertTrue(expectedOutputKeys.contains(entry.getValue().getName()));
        }
    }

    @Test
    public void inputParamTest() {
        Param param = new Param("result1", "string", false, null);
        spyOpt.addParameter(param);
        spyOpt.setParamValue("result1", "value1");

        param = new Param("result2", "string", true, null);
        spyOpt.addParameter(param);
        spyOpt.setParamValue("result2", "value2");

        List<String> expectedParamKeys = new ArrayList<>(Arrays.asList("udfName", "result1", "result2"));
        List<String> expectedValues = new ArrayList<>(Arrays.asList("a name", "value1", "value2"));
        for (Map.Entry<String, Param> entry : spyOpt.getInputParamList().entrySet()) {
            assertTrue(expectedParamKeys.contains(entry.getValue().getName()));
            assertTrue(expectedValues.contains(entry.getValue().getData()));
        }
    }

    @Test
    public void setParamValueTest() {
        sortOpt.setParamValue("partitionNum", "5");
        assertThrows(NoSuchElementException.class, () -> sortOpt.setParamValue("non-exist-param", null));
    }
}
