import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class demo {
    public static void main(String[] args){

        PlanBuilder planBuilder = null;
        try {
            planBuilder = new PlanBuilder();
//            DataQuanta nodeA = planBuilder.readDataFrom("data source file")
//                    .sort();

//            DataQuanta nodeB = nodeA.sort();
//            DataQuanta nodeC = nodeA.map(null, null);
//
//            DataQuanta nodeD = nodeB.collect();
//            nodeD.acceptIncoming(nodeC);

            planBuilder.readDataFrom("../../../resources/sort/data_list.txt")
                    //模拟用户行为：排序
                    .sortTemp()
                    //模拟用户行为：计算每个元素的平方
                    .squareTemp()
                    //相当于collect()
                    .showResult();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
