package demo;

import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @ClassName SparkPCADemo
 * @Description TODO
 * @Author zjchen
 * @Date 2020/12/25 上午11:58
 * @Version 1.0
 **/

public class SparkPCADemo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            String cols = "hotel, is_canceled, lead_time, arrival_date_year, arrival_date_month, "
                    +
                    "arrival_date_week_number, arrival_date_day_of_month, stays_in_weekend_nights, "
                    +
                    "stays_in_week_nights, adults, children, babies, meal, country, market_segment, "
                    +
                    "distribution_channel, is_repeated_guest, previous_cancellations, previous_bookings_not_canceled, "
                    +
                    "reserved_room_type, assigned_room_type, booking_changes, deposit_type, agent, company, "
                    +
                    "days_in_waiting_list, customer_type, adr, required_car_parking_spaces, "
                    +
                    "total_of_special_requests, reservation_status";

            PlanBuilder planBuilder = new PlanBuilder("test-web-case");
            // 设置udf路径   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("java", "/data/udfs/TestSmallWebCaseFunc.class");
            //供测试生成文件使用   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            planBuilder.setPlatformUdfPath("spark", "/data/udfs/TestSmallWebCaseFunc.class");

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
                put("inputPath", "/data/datasets/PCA/hotel_bookings.csv");
                put("header", "True");
                put("infer_schema", "True");
                put("nan_value", "NULL");
            }}).withTargetPlatform("spark-ml");

            DataQuanta dropNode = DataQuanta.createInstance("drop", new HashMap<String, String>() {{
                put("drop_cols", "reservation_status_date");
            }}).withTargetPlatform("spark-ml");

            DataQuanta limitNode = DataQuanta.createInstance("limit", new HashMap<String, String>() {{
                put("number", "50");
            }}).withTargetPlatform("spark-ml");

            DataQuanta standardizationNode = DataQuanta.createInstance("standardization", new HashMap<String, String>() {{
                put("cols", cols);
                put("handle_invalid", "keep");
            }}).withTargetPlatform("spark-ml");

            DataQuanta fillNaNode = DataQuanta.createInstance("fill-na", new HashMap<String, String>() {{
                put("value", "0");
            }}).withTargetPlatform("spark-ml");

            DataQuanta oneHotEncodeNode = DataQuanta.createInstance("one-hot-encode", new HashMap<String, String>() {{
                put("cols", cols);
            }}).withTargetPlatform("spark-ml");

            DataQuanta pcaNode = DataQuanta.createInstance("pca", new HashMap<String, String>() {{
                put("k", "10");
                put("output_col", "PCA_res");
                put("cols", cols);
                put("handle_invalid", "skip");
            }}).withTargetPlatform("spark-ml");


            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(dropNode);
            planBuilder.addVertex(limitNode);
            planBuilder.addVertex(standardizationNode);
            planBuilder.addVertex(fillNaNode);
            planBuilder.addVertex(oneHotEncodeNode);
            planBuilder.addVertex(pcaNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, dropNode);
            planBuilder.addEdge(dropNode, limitNode);
            planBuilder.addEdge(limitNode, standardizationNode);
            planBuilder.addEdge(standardizationNode, fillNaNode);
            planBuilder.addEdge(fillNaNode, oneHotEncodeNode);
            planBuilder.addEdge(oneHotEncodeNode, pcaNode);
            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
