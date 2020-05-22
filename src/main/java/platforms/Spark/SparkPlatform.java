package platforms.Spark;

import basic.Platform;

import java.util.HashMap;
import java.util.Map;

public class SparkPlatform extends Platform {
    private static final Map<String, String> myMap = new HashMap<String, String>(){
        {
            put("SortOperator", "platforms.Spark.Operators.SparkSortOperator");
            put("MapOperator", "platforms.Spark.Operators.SparkMapOperator");
            put("CollectOperator", "platforms.Spark.Operators.SparkCollectOperator");

        }
    };

    @Override
    public String mappingOperator(String origin) {
        return myMap.getOrDefault(origin, null);
    }

}
