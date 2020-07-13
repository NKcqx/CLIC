package basic.traversal;

import basic.operators.Operator;
import channel.Channel;
import org.junit.Before;
import org.junit.Test;

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
         * DAG的初始化
         */
        optA = new Operator("Operator/Source/conf/SourceOperator.xml");
        optB = new Operator("Operator/Map/conf/MapOperator.xml");
        optC = new Operator("Operator/Filter/conf/FilterOperator.xml");
        optD = new Operator("Operator/Sort/conf/SortOperator.xml");
        optE = new Operator("Operator/Sink/conf/SinkOperator.xml");
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
        String optName;
        while (dagTraversal.hasNextOpt()) {
            optTemp = dagTraversal.nextOpt();
            // 依次打印出optName可知DFS遍历成功
            optName = optTemp.getOperatorName();
        }
    }
}
