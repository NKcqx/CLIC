import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @ClassName PytorchDemo
 * @Description TODO
 * @Author zjchen
 * @Date 2020/12/19 下午7:35
 * @Version 1.0
 **/

public class PytorchDemo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            PlanBuilder planBuilder = new PlanBuilder("test-web-case");
            // 设置udf路径   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("java", "/data/udfs/TestSmallWebCaseFunc.class");
            //供测试生成文件使用   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("spark", "/data/udfs/TestSmallWebCaseFunc.class");

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "/Users/zjchen/PycharmProjects/Pytorch/hotel_bookings/hotel_bookings.csv");
            }}).withTargetPlatform("pytorch");

            DataQuanta concatNode = DataQuanta.createInstance("concat", new HashMap<String, String>() {

            }).withTargetPlatform("pytorch");

            DataQuanta dummiesNode = DataQuanta.createInstance("one-hot-encode", new HashMap<String, String>() {{
                put("dummy_na", "True");
            }}).withTargetPlatform("pytorch");

            DataQuanta fillnaNode = DataQuanta.createInstance("fill-na", new HashMap<String, String>() {{
                put("value", "0");
            }}).withTargetPlatform("pytorch");

            DataQuanta ilocNode = DataQuanta.createInstance("loc", new HashMap<String, String>() {{
                put("row_from", "None");
                put("row_to", "500");
                put("col_from", "None");
                put("col_to", "-1");
            }}).withTargetPlatform("pytorch");

            DataQuanta standardizationNode = DataQuanta.createInstance("standardization", new HashMap<String, String>() {

            }).withTargetPlatform("pytorch");

            DataQuanta toTensorNode = DataQuanta.createInstance("to-tensor", new HashMap<String, String>() {{
                put("dtype", "torch.float");
            }}).withTargetPlatform("pytorch");

            DataQuanta pcaNode = DataQuanta.createInstance("pca", new HashMap<String, String>() {{
                put("k", "10");
                put("center", "True");
            }}).withTargetPlatform("pytorch");


            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(dummiesNode);
            planBuilder.addVertex(fillnaNode);
            planBuilder.addVertex(ilocNode);
            planBuilder.addVertex(toTensorNode);
            planBuilder.addVertex(pcaNode);
            planBuilder.addVertex(standardizationNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, ilocNode);
            planBuilder.addEdge(ilocNode, standardizationNode);
            planBuilder.addEdge(standardizationNode, fillnaNode);
            planBuilder.addEdge(fillnaNode, dummiesNode);
            planBuilder.addEdge(dummiesNode, toTensorNode);
            planBuilder.addEdge(toTensorNode, pcaNode);
            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
