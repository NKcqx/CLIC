import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.OperatorStructure;
import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.base.PlanNode;
import fdu.daslab.thrift.jobcenter.JobService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.Test;

import java.util.*;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 8:46 PM
 */
public class TestJob {

    @Test
    public void test() throws TException {
        TSocket transport = new TSocket("localhost", 4000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        final JobService.Client client = new JobService.Client(protocol);
        if (!transport.isOpen()) transport.open();
        try {
            Plan plan = new Plan();
            // 构建一个多平台的plan
            Operator operator1 = new Operator();
            operator1.setOperatorStructure(OperatorStructure.SOURCE);
            PlanNode node1 = new PlanNode(1, operator1, null,
                    new ArrayList<>(Collections.singletonList(2)),
                    "spark");
            Operator operator2 = new Operator();
            operator2.setOperatorStructure(OperatorStructure.MAP);
            PlanNode node2 = new PlanNode(2, operator2, new ArrayList<>(Collections.singletonList(1)),
                    new ArrayList<>(Collections.singletonList(3)),
                    "spark");
            Operator operator3 = new Operator();
            operator3.setOperatorStructure(OperatorStructure.MAP);
            PlanNode node3 = new PlanNode(3, operator3, new ArrayList<>(Collections.singletonList(2)),
                    new ArrayList<>(Collections.singletonList(4)),
                    "tensorflow");
            Operator operator4 = new Operator();
            operator4.setOperatorStructure(OperatorStructure.SINK);
            PlanNode node4 = new PlanNode(4, operator4, new ArrayList<>(Collections.singletonList(3)),null,
                    "tensorflow");
            plan.setNodes(new HashMap<Integer, PlanNode>(){{
                put(1, node1); put(2, node2); put(3, node3); put(4, node4);
            }});
            plan.setSourceNodes(new ArrayList<>(Collections.singletonList(1)));
            client.submit(plan, "test");
        } finally {
            transport.close();
        }
    }
}
