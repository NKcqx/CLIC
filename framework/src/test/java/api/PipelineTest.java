package api;

import adapters.ArgoAdapter;
import basic.Configuration;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import basic.platforms.PlatformFactory;
import fdu.daslab.backend.executor.model.Pipeline;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Testing for Pipeline.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:42
 */
public class PipelineTest {

    private Pipeline spyArgoPipeline;
    private Pipeline argoPipeline;

    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
        PlatformFactory.initMapping(configuration.getProperty("platform-mapping-file"));

        Operator opt1 = OperatorFactory.createOperator("source");
        Operator opt2 = OperatorFactory.createOperator("filter");
        Operator opt3 = OperatorFactory.createOperator("map");
        Operator opt4 = OperatorFactory.createOperator("reducebykey");
        Operator opt5 = OperatorFactory.createOperator("sort");
        Operator opt6 = OperatorFactory.createOperator("sink");

        opt1.selectEntity("java");
        opt2.selectEntity("java");
        opt3.selectEntity("spark");
        opt4.selectEntity("java");
        opt5.selectEntity("java");
        opt6.selectEntity("java");

//        opt1.connectTo(new Channel(opt1, opt2, null));
//        opt2.connectFrom(new Channel(opt1, opt2, null));
//        opt2.connectTo(new Channel(opt2, opt3, null));
//        opt3.connectFrom(new Channel(opt2, opt3, null));
//        opt3.connectTo(new Channel(opt3, opt4, null));
//        opt4.connectFrom(new Channel(opt3, opt4, null));
//        opt4.connectTo(new Channel(opt4, opt5, null));
//        opt5.connectFrom(new Channel(opt4, opt5, null));
//        opt5.connectTo(new Channel(opt5, opt6, null));
//        opt6.connectFrom(new Channel(opt5, opt6, null));

        List<Operator> allOperators = new ArrayList<>();
        allOperators.add(opt1);
        allOperators.add(opt2);
        allOperators.add(opt3);
        allOperators.add(opt4);
        allOperators.add(opt5);
        allOperators.add(opt6);
        argoPipeline = new Pipeline(new ArgoAdapter(), allOperators);
        spyArgoPipeline = spy(argoPipeline);
    }

    @Test
    public void execute() {
        spyArgoPipeline.execute();
        verify(spyArgoPipeline, times(1)).execute();
    }
}
