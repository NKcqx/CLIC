package platforms.Java;

import basic.Platform;
import platforms.Java.Operators.JavaCollectOperator;
import platforms.Java.Operators.JavaFilterOperator;
import platforms.Java.Operators.JavaMapOperator;
import platforms.Java.Operators.JavaSortOperator;

import java.util.HashMap;
import java.util.Map;

public class JavaPlatform extends Platform {
    private static final Map<String, String> myMap = new HashMap<String, String>(){
        {
            /*put("SortOperator", String.valueOf(JavaSortOperator.class).substring(5));
            put("MapOperator", String.valueOf(JavaMapOperator.class).substring(5));
            put("CollectOperator", String.valueOf(JavaCollectOperator.class).substring(5));
            put("FilterOperator", String.valueOf(JavaFilterOperator.class).substring(5) "platforms.Java.Operators.JavaFilterOperator");*/

            put("SortOperator", String.valueOf(JavaSortOperator.class).substring(6));
            put("MapOperator", String.valueOf(JavaMapOperator.class).substring(6));
            put("CollectOperator", String.valueOf(JavaCollectOperator.class).substring(6));
            put("FilterOperator", String.valueOf(JavaFilterOperator.class).substring(6));
        }
    };

    @Override
    public String mappingOperator(String origin) {
        return myMap.getOrDefault(origin, null);
    }

}
