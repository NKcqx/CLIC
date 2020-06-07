import api.DataQuanta;
import api.PlanBuilder;


public class demo {
    public static void main(String[] args){
        try {

            PlanBuilder planBuilder = new PlanBuilder();
            DataQuanta nodeA = planBuilder
                    .readDataFrom("data source file")
                    .then("sort");

            DataQuanta nodeB = nodeA.then("filter");

            DataQuanta nodeC = nodeA.then("map");

            // nodeD的第一个输入
            DataQuanta nodeD = nodeB.then("collect");
            // nodeD的第2个输入
            nodeD.acceptIncoming(nodeC);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
