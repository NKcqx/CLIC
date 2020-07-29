package basic.operators;

import basic.Param;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Testing for Operator.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/10 0:52
 */
public class OperatorTest {

    private String configFilePath;
    private Operator spyOpt;

    @Before
    public void before() throws Exception {
        configFilePath = "Operator/Filter/conf/FilterOperator.xml";
        spyOpt = spy(OperatorFactory.createOperatorFromFile(configFilePath));
    }

    @Test
    public void simpleConstructorTest() throws Exception {
        Operator opt = OperatorFactory.createOperatorFromFile("Operator/Filter/conf/FilterOperator.xml");
    }

    @Test
    public void inputDataListTest() {

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

        String name = "inputKey";
        String dataType = "string";
        String defaultValue = null;
        Boolean isRequired = false;
        Param param = new Param(name, dataType, isRequired, defaultValue);
        Map<String, Param> inputDataList = spyOpt.getInputParamList();
        inputDataList.put(name, param);

        spyOpt.setParamValue("inputKey", "inputData");

        String[] expectedNames = {"inputKey", "udfName"};
        String[] expectedDatas = {"inputData", null};
        String[] actualNames = new String[2];
        String[] actualDatas = new String[2];

        int i = 0;
        for (Map.Entry<String, Param> entry : spyOpt.getInputParamList().entrySet()) {
            actualNames[i] = entry.getValue().getName();
            actualDatas[i] = entry.getValue().getData();
            i += 1;
        }

        assertArrayEquals(expectedNames, actualNames);
        assertArrayEquals(expectedDatas, actualDatas);
        verify(spyOpt, times(1)).setParamValue("inputKey", "inputData");
    }

    @Test
    public void outputDataTest() {

        String name = "outputKey";
        String dataType = "string";
        String defaultValue = null;
        Boolean isRequired = false;
        Param param = new Param(name, dataType, isRequired, defaultValue);
        Map<String, Param> outputDataList = spyOpt.getOutputDataList();
        outputDataList.put(name, param);

        spyOpt.setParamValue("outputKey", "outputData");

        String[] expectedNames = {"result", "outputKey"};
        String[] expectedDatas = {null, "outputData"};
        String[] actualNames = new String[2];
        String[] actualDatas = new String[2];

        int i = 0;
        for (Map.Entry<String, Param> entry : spyOpt.getOutputDataList().entrySet()) {
            actualNames[i] = entry.getValue().getName();
            actualDatas[i] = entry.getValue().getData();
            i += 1;
        }

        assertArrayEquals(expectedNames, actualNames);
        assertArrayEquals(expectedDatas, actualDatas);
        verify(spyOpt, times(1)).setParamValue("outputKey", "outputData");
    }
}
