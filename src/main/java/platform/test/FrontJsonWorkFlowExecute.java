package platform.test;

import api.DataQuanta;
import api.PlanBuilder;
import org.apache.spark.sql.Row;
import org.xml.sax.SAXException;
import platform.spark.FrontJsonParse.FrontJsonParse;
import platform.spark.SparkPlatform;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class FrontJsonWorkFlowExecute {
    public static void workFlowExecute() throws IOException, SAXException, ParserConfigurationException {
        PlanBuilder planBuilder = new PlanBuilder();
        FrontJsonParse frontJsonParse = new FrontJsonParse();
        frontJsonParse.parseJson("WorkFlowTest.json");
        frontJsonParse.connectDAG();
        HashMap<Integer, DataQuanta> nodeList = frontJsonParse.getNodeOrderDataQuanta();
        planBuilder.setHeadDataQuanta(nodeList.get(1));
        List<Row> object=(List<Row>) SparkPlatform.SparkRunner(planBuilder);
        int numRow= (int) (object.size()*0.0001);
        for(int i=0;i<numRow;i++){
            System.out.println(object.get(i));
        }
    }

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
        FrontJsonWorkFlowExecute frontJsonWorkFlowExecute = new FrontJsonWorkFlowExecute();
        frontJsonWorkFlowExecute.workFlowExecute();
    }

}


