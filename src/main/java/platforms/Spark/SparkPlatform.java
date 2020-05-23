package platforms.Spark;

import basic.Platform;
import platforms.Spark.Operators.SparkCollectOperator;
import platforms.Spark.Operators.SparkFilterOperator;
import platforms.Spark.Operators.SparkMapOperator;
import platforms.Spark.Operators.SparkSortOperator;

import java.util.HashMap;
import java.util.Map;

public class SparkPlatform extends Platform {
    private static final Map<String, String> myMap = new HashMap<String, String>(){
        {
            put("SortOperator", String.valueOf(SparkSortOperator.class).substring(6)); // 删掉前面的 "class "
            put("MapOperator", String.valueOf(SparkMapOperator.class).substring(6));
            put("CollectOperator", String.valueOf(SparkCollectOperator.class).substring(6));
            put("FilterOperator", String.valueOf(SparkFilterOperator.class).substring(6));

        }
    };

    @Override
    public String mappingOperator(String origin) {
        return myMap.getOrDefault(origin, null);
    }

}
