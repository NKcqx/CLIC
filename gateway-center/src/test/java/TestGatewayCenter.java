import fdu.daslab.gatewaycenter.service.JobWebService;
import fdu.daslab.gatewaycenter.utils.PlanBuilder;
import fdu.daslab.thrift.base.Plan;
import org.apache.thrift.TException;
import org.junit.Test;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;

import java.io.File;
import java.io.IOException;

/**
 * @author zjchenn
 * @description
 * @since 2021/6/16 下午2:37
 */
public class TestGatewayCenter {


//    @Test
//    public void testParseJson() throws IOException {
//        File file = new File("/Users/zjchen/Desktop/CLIC/PytorchCodes/pytorchjob.json");
//        String content= FileUtils.readFileToString(file,"UTF-8");
//        PlanBuilder planBuilder = new PlanBuilder();
//        Plan plan = planBuilder.parseJson(content);
//        System.out.println(plan);
//    }
//
//    @Test
//    public void testJobServiceSubmit() throws IOException, TException {
//        File file = new File("/Users/zjchen/Codes/JavaProjects/CLIC/gateway-center/src/main/resources/templatePlan.json");
//        String planJsonString= FileUtils.readFileToString(file,"UTF-8");
////        JobWebService jobWebService = new JobWebService();
////        jobWebService.submit("testJobName", planJsonString);
//    }
}
