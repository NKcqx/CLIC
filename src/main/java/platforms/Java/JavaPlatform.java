package platforms.Java;

import basic.Platform;

import java.util.HashMap;
import java.util.Map;

public class JavaPlatform extends Platform {
    private static final Map<String, String> myMap = new HashMap<String, String>(){
        {
            put("SortOperator", "platforms.Java.Operators.JavaSortOperator");
            put("MapOperator", "platforms.Java.Operators.JavaMapOperator");
            put("CollectOperator", "platforms.Java.Operators.JavaCollectOperator");
        }
    };

    @Override
    public String mappingOperator(String origin) {
        return myMap.getOrDefault(origin, null);
    }

}
