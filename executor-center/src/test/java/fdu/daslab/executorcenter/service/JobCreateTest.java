package fdu.daslab.executorcenter.service;

import fdu.daslab.thrift.base.*;
import fdu.daslab.thrift.executorcenter.ExecutorService;
import fdu.daslab.thrift.operatorcenter.OperatorCenter;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/7/21 8:33 PM
 */
public class JobCreateTest {

    @Test
    public void insertPlatform() {
        TSocket transport = new TSocket("localhost", 4000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        OperatorCenter.Client operatorClient = new OperatorCenter.Client(protocol);
        Platform platform = new Platform();
        platform.name = "java";
        platform.useOperator = false;
        platform.defaultImage = "java-env:v0";
        platform.execCommand = "/bin/sh -c java -jar /data/jars/executable-java.jar";
        // 下面属于stage的信息，不属于平台信息
//        platform.params = new HashMap<String, String>() {{
//            put("udfPath", "/data/udfs/java.class");
//        }};
        try {
            transport.open();
            operatorClient.addPlatform(platform);
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }

    @Test
    public void test() {
        TSocket transport = new TSocket("localhost", 7000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        ExecutorService.Client client = new ExecutorService.Client(protocol);
        Stage stage = new Stage();
        Plan plan = new Plan();
        Operator operator1 = new Operator();
        operator1.setName("SourceOperator");
        operator1.setOperatorStructure(OperatorStructure.SOURCE);
        operator1.setInputKeys(new ArrayList<>());
        operator1.setOutputKeys(Arrays.asList("result"));
        PlanNode node1 = new PlanNode(1, operator1, null,
                new ArrayList<>(Collections.singletonList(2)),
                "Java");
        Operator operator2 = new Operator();
        operator2.setName("MapOperator");
        operator2.setOperatorStructure(OperatorStructure.MAP);
        operator2.setInputKeys(Arrays.asList("data"));
        operator2.setOutputKeys(Arrays.asList("result"));
        PlanNode node2 = new PlanNode(2, operator2, new ArrayList<>(Collections.singletonList(1)),
                new ArrayList<>(Collections.singletonList(3)),
                "Java");
        Operator operator3 = new Operator();
        operator3.setName("SinkOperator");
        operator3.setOperatorStructure(OperatorStructure.SINK);
        operator3.setInputKeys(Arrays.asList("data"));
        operator3.setOutputKeys(new ArrayList<>());
        PlanNode node3 = new PlanNode(2, operator3, new ArrayList<>(Collections.singletonList(2)),null,
                "Java");
        plan.setNodes(new HashMap<Integer, PlanNode>(){{
            put(1, node1); put(2, node2); put(3, node3);
        }});
        plan.setSourceNodes(new ArrayList<>(Collections.singletonList(1)));
        stage.planInfo = plan;
        stage.stageId = 100;
        stage.outputStageId = Arrays.asList(200, 300);
        stage.platformName = "Java";
        stage.jobName = "planA";
        try {
            transport.open();
            client.executeStage(stage);
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }
}
