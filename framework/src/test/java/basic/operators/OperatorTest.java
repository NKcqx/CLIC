package basic.operators;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Testing for Operator.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/10 0:52
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Operator.class)
public class OperatorTest {

    @Mock
    private Operator operator;

    @Before
    public void before() {
        /**
         * Solve the contradiction between junit and System.getProperty("user.dir")
         */
        String userDir = "user.dir";
        System.setProperty(userDir, "D:\\IRDemo\\");
    }

    @Test
    public void simpleConstructorTest() throws Exception {
        Operator opt = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
    }

    @Test
    public void constructorTest() throws Exception {
        String configFilePath = "/framework/resources/Operator/Filter/conf/FilterOperator.xml";
        PowerMockito.whenNew(Operator.class).withArguments(configFilePath).thenReturn(operator);
        PowerMockito.doNothing().when(operator).loadOperatorConfs();
        Operator realOperator = new Operator(configFilePath);
        realOperator.loadOperatorConfs();

        // Both verifies had run
        Mockito.verify(realOperator).loadOperatorConfs();
        Mockito.verify(operator).loadOperatorConfs();
    }

    @Test
    // TODO
    public void loadBasicInfoTest() throws Exception {
        Operator opt = PowerMockito.spy(
                new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml"));
        PowerMockito.doNothing().when(opt, "loadParams");
        PowerMockito.verifyPrivate(opt, Mockito.times(0)).invoke("loadBasicInfo");
    }

    @Test
    // TODO
    public void loadParamsTest() throws Exception {
        Operator spyOpt = PowerMockito.spy(
                new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml"));
        PowerMockito.doNothing().when(spyOpt, "loadParams");
        PowerMockito.verifyPrivate(spyOpt, Mockito.times(0)).invoke("loadParams");
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
}
