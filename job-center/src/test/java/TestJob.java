//import fdu.daslab.thrift.base.Operator;
//import fdu.daslab.thrift.base.OperatorStructure;
//import fdu.daslab.thrift.base.Plan;
//import fdu.daslab.thrift.base.PlanNode;
//import fdu.daslab.thrift.jobcenter.JobService;
//import fdu.daslab.thrift.operatorcenter.OperatorCenter;
//import org.apache.thrift.TException;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.transport.TSocket;
//import org.junit.Test;
//
//import java.util.*;
//
///**
// * @author 唐志伟
// * @version 1.0
// * @since 5/18/21 8:46 PM
// */
//public class TestJob {
//
//    @Test
//    public void test() throws TException {
//        TSocket transport = new TSocket("localhost", 3000);
//        TSocket transport2 = new TSocket("localhost", 4000);
//        TBinaryProtocol protocol = new TBinaryProtocol(transport);
//        final JobService.Client client = new JobService.Client(protocol);
//        final OperatorCenter.Client operatorClient = new OperatorCenter.Client(new TBinaryProtocol(transport2));
//        try {
//            transport.open();
//            transport2.open();
//            Plan plan = new Plan();
//            plan.others.put("udfPath-Java",
//                    "/Users/edward/Code/Lab/Clic/executable-operator/executable-basic/target/classes/fdu/daslab/executable/udf/TestFunc.class"); // 多语言情况？
//            // 构建一个多平台的plan
//            Operator operator1 = operatorClient.findOperatorInfo("SourceOperator",
//                    null);
//            operator1.params.put("inputPath", "/Users/edward/Code/Lab/data/test.csv");
//            operator1.params.put("separator", ",");
//            PlanNode node1 = new PlanNode(1, operator1, null,
//                    new ArrayList<>(Collections.singletonList(2)),
//                    "java");
//            Operator operator2 = operatorClient.findOperatorInfo("MapOperator", null);
//            operator2.params.put("udfName", "mapFunc");
//            PlanNode node2 = new PlanNode(2, operator2, new ArrayList<>(Collections.singletonList(1)),
//                    new ArrayList<>(Collections.singletonList(3)),
//                    "java");
//            Operator operator3 = operatorClient.findOperatorInfo("FilterOperator", null);
//            operator3.params.put("udfName", "filterFunc");
//            operator3.setOperatorStructure(OperatorStructure.MAP);
//            PlanNode node3 = new PlanNode(3, operator3, new ArrayList<>(Collections.singletonList(2)),
//                    new ArrayList<>(Collections.singletonList(4)),
//                    "java");
//            Operator operator4 = operatorClient.findOperatorInfo("SinkOperator", null);
//            operator4.params.put("separator", ",");
//            operator4.params.put("outputPath", "/Users/edward/Code/Lab/data/output-temp.csv");
//            PlanNode node4 = new PlanNode(4, operator4, new ArrayList<>(Collections.singletonList(3)), null,
//                    "java");
//            plan.setNodes(new HashMap<Integer, PlanNode>() {{
//                put(1, node1);
//                put(2, node2);
//                put(3, node3);
//                put(4, node4);
//            }});
//            plan.setSourceNodes(new ArrayList<>(Collections.singletonList(1)));
//            client.submit(plan, "test");
//        } finally {
//            transport.close();
//            transport2.close();
//        }
//    }
//}
