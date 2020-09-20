package platform.test;

//import api.DataQuanta;
//import api.PlanBuilder;
//import org.apache.spark.sql.Row;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.xml.sax.SAXException;
//import platform.spark.SparkPlatform;
//import platform.spark.front.FrontJsonParse;
//
//import javax.xml.parsers.ParserConfigurationException;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//
//public class FrontJsonWorkFlowExecute {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(FrontJsonWorkFlowExecute.class);
//
//    public static void workFlowExecute() throws IOException, SAXException, ParserConfigurationException {
//        PlanBuilder planBuilder = new PlanBuilder();
//        FrontJsonParse frontJsonParse = new FrontJsonParse();
//        frontJsonParse.parseJson("WorkFlowTest.json");
//        frontJsonParse.connectDAG();
//        HashMap<Integer, DataQuanta> nodeList = frontJsonParse.getNodeOrderDataQuanta();
//        planBuilder.setHeadDataQuanta(nodeList.get(1));
//        List<Row> object = (List<Row>) SparkPlatform.sparkRunner(planBuilder);
//        int numRow = (int) (object.size() * 0.0001);
//        for (int i = 0; i < numRow; i++) {
//            LOGGER.info(object.get(i).toString());
//        }
//    }
//
//    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
//        FrontJsonWorkFlowExecute frontJsonWorkFlowExecute = new FrontJsonWorkFlowExecute();
//        frontJsonWorkFlowExecute.workFlowExecute();
//    }
//
//}


