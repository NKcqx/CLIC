package platform.spark.visitor;

import java.util.HashMap;
import java.util.Map;

public class SparkVisitorConfiguration {

    public static final Map<String, Class> OP_MAP = new HashMap<String, Class>() {{
        put("SourceOperator", DataSourceVisitor.class);
        put("JoinOperator", JoinVisitor.class);
        put("ProjectOperator", ProjectVisitor.class);
        put("FilterOperator", FilterVisitor.class);
        put("CollectOperator", CollectVisitor.class);
    }};

}
