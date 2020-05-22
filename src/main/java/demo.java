import api.PlanBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class demo {
    public static void main(String[] args){
        Supplier<String> mapUDF = () -> {
            System.out.println("*****   Hello World   *****");
            return "";
        };

        PlanBuilder planBuilder = new PlanBuilder();
        planBuilder
                .map(mapUDF, "MapOperator")
                .sort("SortOperator")
                .collect();
        try {

            planBuilder.execute();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
