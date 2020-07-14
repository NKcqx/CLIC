package basic.traversal;

import basic.operators.Operator;
import channel.Channel;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/11 17:07
 */
public class TopTraversalTest {

    /**
     * 测试DAG的节点
     */
    private Operator optA;
    private Operator optB;
    private Operator optC;
    private Operator optD;
    private Operator optE;
    private Operator optF;
    private Operator optG;
    private Operator optH;
    private Operator optI;
    private Operator optJ;


    /**
     * 测试DAG的边
     */
    private Channel aTb;
    private Channel aTc;
    private Channel aTd;
    private Channel bTe;
    private Channel dTf;
    private Channel dTg;
    private Channel fTi;
    private Channel eTh;
    private Channel hTj;

    @Before
    public void before() throws Exception {

        /**
         * DAG的初始化
         */
        optA = new Operator("Operator/Source/conf/SourceOperator.xml");
        optB = new Operator("Operator/Map/conf/MapOperator.xml");
        optC = new Operator("Operator/Sort/conf/SortOperator.xml");
        optD = new Operator("Operator/Filter/conf/FilterOperator.xml");
        optE = new Operator("Operator/Map/conf/MapOperator.xml");
        optF = new Operator("Operator/ReduceByKey/conf/ReduceByKeyOperator.xml");
        optG = new Operator("Operator/Join/conf/JoinOperator.xml");
        optH = new Operator("Operator/Source/conf/SourceOperator.xml");
        optI = new Operator("Operator/Sink/conf/SinkOperator.xml");
        optJ = new Operator("Operator/Map/conf/MapOperator.xml");
        aTb = new Channel(optA, optB, null);
        aTc = new Channel(optA, optC, null);
        aTd = new Channel(optA, optD, null);
        bTe = new Channel(optB, optE, null);
        dTf = new Channel(optD, optF, null);
        dTg = new Channel(optD, optG, null);
        eTh = new Channel(optE, optH, null);
        fTi = new Channel(optF, optI, null);
        hTj = new Channel(optH, optJ, null);
        optA.connectTo(aTb);
        optB.connectFrom(aTb);
        optA.connectTo(aTc);
        optC.connectFrom(aTc);
        optA.connectTo(aTd);
        optD.connectFrom(aTd);
        optB.connectTo(bTe);
        optE.connectFrom(bTe);
        optD.connectTo(dTf);
        optF.connectFrom(dTf);
        optD.connectTo(dTg);
        optG.connectFrom(dTg);
        optE.connectTo(eTh);
        optH.connectFrom(eTh);
        optF.connectTo(fTi);
        optI.connectFrom(fTi);
        optH.connectTo(hTj);
        optJ.connectFrom(hTj);
    }

    @Test
    public void topTraversalTest() {
        AbstractTraversal dagTraversal = new TopTraversal(optA);
        Operator optTemp;
        String optName;
        String[] expectedNames = {
                "SourceOperator", "MapOperator", "SortOperator", "FilterOperator", "MapOperator",
                "ReduceByKeyOperator", "JoinOperator", "SourceOperator", "SinkOperator", "MapOperator"
        };
        String[] optNames = new String[10];

        int i = 0;
        while (dagTraversal.hasNextOpt()) {
            optTemp = dagTraversal.nextOpt();
            optName = optTemp.getOperatorName();
            optNames[i] = optName;
            i += 1;
        }
        assertArrayEquals(expectedNames, optNames);
    }
}
