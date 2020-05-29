import api.DataQuanta;
import api.PlanBuilder;

import java.util.function.Supplier;

public class demo {
    public static void main(String[] args){
        Supplier s = () -> null;
        try {
            PlanBuilder planBuilder = new PlanBuilder();
            DataQuanta nodeA = planBuilder.readDataFrom("data source file").sort();
            DataQuanta nodeB = nodeA.filter();
            DataQuanta nodeC = nodeA.map(s, "map");
            DataQuanta nodeD = nodeB.sort();
            nodeD.acceptIncoming(nodeC);

            planBuilder.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
