import api.DataQuanta;
import api.PlanBuilder;

import java.util.function.Supplier;

public class demo {
    public static void main(String[] args){
        Supplier s = () -> null;
        try {

            PlanBuilder planBuilder = new PlanBuilder();
//            DataQuanta nodeA = planBuilder.readDataFrom("data source file").sort();
//            DataQuanta nodeB = nodeA.filter();
//            DataQuanta nodeC = nodeA.map(s, "map");
//            DataQuanta nodeD = nodeB.sort();
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
