package basic.traversal;

import basic.operators.Operator;
import channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Testing for DfsTraversal.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/11 17:03
 */
public class DfsTraversalTest {

    /**
     * 测试DAG的节点
     */
    private Operator optA;
    private Operator optB;
    private Operator optC;
    private Operator optD;
    private Operator optE;

    /**
     * 测试DAG的边
     */
    private Channel aTb;
    private Channel aTc;
    private Channel cTd;
    private Channel cTe;

    @Before
    public void before() throws Exception {
        /**
         * Solve the contradiction between junit and System.getProperty("user.dir")
         */
        String userDir = "user.dir";
        // 下面路径根据本地实际情况改，只要到项目根目录就行
        System.setProperty(userDir, "D:\\IRDemo\\");

        /**
         * DAG的初始化
         */
        optA = new Operator("/framework/resources/Operator/Source/conf/SourceOperator.xml");
        optB = new Operator("/framework/resources/Operator/Map/conf/MapOperator.xml");
        optC = new Operator("/framework/resources/Operator/Filter/conf/FilterOperator.xml");
        optD = new Operator("/framework/resources/Operator/Sort/conf/SortOperator.xml");
        optE = new Operator("/framework/resources/Operator/Sink/conf/SinkOperator.xml");
        aTb = new Channel(optA, optB, null);
        aTc = new Channel(optA, optC, null);
        cTd = new Channel(optC, optD, null);
        cTe = new Channel(optC, optE, null);
        optA.connectTo(aTb);
        optB.connectFrom(aTb);
        optA.connectTo(aTc);
        optC.connectFrom(aTc);
        optC.connectTo(cTd);
        optD.connectFrom(cTd);
        optC.connectTo(cTe);
        optE.connectFrom(cTe);
    }

    @Test
    public void dfsTraversalTest() {
        AbstractTraversal dagTraversal = new DfsTraversal(optA);
        Operator optTemp;
        Operator spyOptTemp;
        String optName;
        while (dagTraversal.hasNextOpt()) {
            optTemp = dagTraversal.nextOpt();
            // 用Powermock构建Spy类
            spyOptTemp = PowerMockito.spy(optTemp);
            // 用Whitebox获取私有成员
            optName = Whitebox.getInternalState(spyOptTemp, "operatorName");
            // 依次打印出optName可知DFS遍历成功
        }
    }
}
