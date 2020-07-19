package api;

import adapters.ArgoAdapter;
import basic.operators.Operator;
import fdu.daslab.backend.executor.model.Pipeline;
import fdu.daslab.backend.executor.utils.YamlUtil;
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

    @Before
    public void setUp() throws Exception {
        List<Operator> allOperators = new ArrayList<>();
        Operator opt1 = new Operator("Operator/Source/conf/SourceOperator.xml");
        opt1.selectEntity("java");
        Operator opt2 = new Operator("Operator/Map/conf/MapOperator.xml");
        opt2.selectEntity("spark");
        Operator opt3 = new Operator("Operator/Sink/conf/SinkOperator.xml");
        opt3.selectEntity("java");
        allOperators.add(opt1);
        allOperators.add(opt2);
        allOperators.add(opt3);
        spyArgoPipeline = spy(new Pipeline(new ArgoAdapter(), allOperators));
    }

    @Test
    public void execute() {
//        spyArgoPipeline.execute();
//        verify(spyArgoPipeline, times(1)).execute();
    }
}
