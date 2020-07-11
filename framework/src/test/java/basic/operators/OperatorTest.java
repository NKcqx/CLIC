package basic.operators;

import basic.Param;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.w3c.dom.Document;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Testing for Operator.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/10 0:52
 */
@RunWith(PowerMockRunner.class) // 指定run的是PowerMockRunner
@PrepareForTest(Operator.class) // 所有要测试的类（本例只有Operator.java）
@PowerMockIgnore({"javax.management.*"}) // 解决使用Powermock之后的classloader错误
public class OperatorTest {

    @Before
    public void before() {
        /**
         * Solve the contradiction between junit and System.getProperty("user.dir")
         */
        String userDir = "user.dir";
        // 下面路径根据本地实际情况改，只要到项目根目录就行
        System.setProperty(userDir, "D:\\IRDemo\\");
    }

    @Test
    public void simpleConstructorTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
    }

    @Test
    public void constructorTest() throws Exception {
        String configFilePath = "/framework/resources/Operator/Filter/conf/FilterOperator.xml";
        Operator spyOpt = PowerMockito.spy(
                new Operator(configFilePath));
        PowerMockito.whenNew(Operator.class).withArguments(configFilePath).thenReturn(spyOpt);
        Operator realOperator = new Operator(configFilePath);
        realOperator.loadOperatorConfs();

        // Both verifies had run
        // realOperator已变成mock类型对象
        Mockito.verify(realOperator).loadOperatorConfs();
        Mockito.verify(spyOpt).loadOperatorConfs();
    }

    @Test
    // TODO loadBasicInfo函数只在构造函数中调用，不方便进行Test
    public void loadBasicInfoTest() throws Exception {
        Operator opt = PowerMockito.spy(
                new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml"));
        PowerMockito.doNothing().when(opt, "loadParams");
        PowerMockito.verifyPrivate(opt, Mockito.times(0)).invoke("loadBasicInfo");
    }

    @Test
    // TODO loadParams函数只在构造函数中调用，不方便进行Test
    public void loadParamsTest() throws Exception {
        Operator spyOpt = PowerMockito.spy(
                new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml"));
        PowerMockito.doNothing().when(spyOpt, "loadParams");
        PowerMockito.verifyPrivate(spyOpt, Mockito.times(0)).invoke("loadParams");
    }

    @Test
    public void inputDataListTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);
        String name = "udfName2";
        String dataType = "string";
        String defaultValue = null;
        Boolean isRequired = false;
        Param param = new Param(name, dataType, isRequired, defaultValue);
        Map<String, Param> inputDataList = spyOpt.getInputDataList();
        inputDataList.put(name, param);

        // 使用Whitebox去访问私有成员
        // 实际上getInputDataList()函数已让inputDataList可对外访问，Whitebox这行可以省略
        Whitebox.setInternalState(spyOpt, "inputDataList", inputDataList);

        for (Map.Entry<String, Param> entry : spyOpt.getInputDataList().entrySet()) {
            // 打印出inputDataList可发现put函数调用成功
            String entryKey = entry.getKey();
        }
    }

    @Test
    public void loadOperatorConfsTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);
        spyOpt.loadOperatorConfs();
        verify(spyOpt, times(1)).loadOperatorConfs();
    }

    @Test
    public void getPlatformOptConfTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);
        spyOpt.getPlatformOptConf();
        verify(spyOpt, times(1)).getPlatformOptConf();
    }

    @Test
    public void loadImplementsTest1() throws Exception {
        Operator spyOpt = PowerMockito.spy(
                new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml"));
        spyOpt.getPlatformOptConf();
        PowerMockito.verifyPrivate(spyOpt, Mockito.times(2))
                .invoke("loadImplements", Mockito.any(Document.class));
    }

    @Test
    public void loadImplementsTest2() throws Exception {
        Operator spyOpt = PowerMockito.spy(
                new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml"));
        PowerMockito.doAnswer((Answer) invocation -> {
            return null;
        }).when(spyOpt, "loadImplements", Mockito.any(Document.class));
        spyOpt.getPlatformOptConf();
        PowerMockito.verifyPrivate(spyOpt, Mockito.times(2))
                .invoke("loadImplements", Mockito.any(Document.class));
    }

    @Test
    public void selectEntityTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);
        spyOpt.selectEntity("java");
        verify(spyOpt, times(1)).selectEntity("java");
        verify(spyOpt, times(0)).selectEntity("spark");
    }

    @Test
    public void evaluateTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);
        boolean flag = spyOpt.evaluate();
        assertEquals(true, flag);
        Mockito.when(spyOpt.evaluate()).thenReturn(false);
        boolean mockFlag = spyOpt.evaluate();
        assertEquals(false, mockFlag);
        verify(spyOpt, times(3)).evaluate();
    }

    @Test
    public void tempPrepareDataTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);
        spyOpt.tempPrepareData();
        verify(spyOpt, times(1)).tempPrepareData();
    }

    @Test
    public void tempDoEvaluateTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);
        spyOpt.tempDoEvaluate();
        verify(spyOpt, times(1)).tempDoEvaluate();
    }

    @Test
    public void setDataAndSetInputDataTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        Operator spyOpt = Mockito.spy(opt);

        //spyOpt.setData("inputKey", "inputData"); // 此时会抛出NoSuchElementException“未在配置文件中...”

        String name = "inputKey";
        String dataType = "string";
        String defaultValue = null;
        Boolean isRequired = false;
        Param param = new Param(name, dataType, isRequired, defaultValue);
        Map<String, Param> inputDataList = spyOpt.getInputDataList();
        inputDataList.put(name, param);

        spyOpt.setData("inputKey", "inputData");

        for (Map.Entry<String, Param> entry : spyOpt.getInputDataList().entrySet()) {
            // 打印出inputDataList可发现setData函数和setInputData函数调用成功
            String entryKey = entry.getKey();
        }

        verify(spyOpt, times(1)).setData("inputKey", "inputData");
    }
}
