import api.DataQuanta;
import api.PlanBuilder;

import java.util.HashMap;


public class demo {
    public static void main(String[] args){
        try {

            PlanBuilder planBuilder = new PlanBuilder();
            // 创建节点
            DataQuanta source_node = planBuilder.readDataFrom(new HashMap<String, String>(){{
                put("data_path", "source data file path");
            }});
            DataQuanta sort_node = DataQuanta.createInstance("sort", new HashMap<String, String>(){{
                put("is_reverse", "false");
                put("sorted_value", "sort sorted_value file path");
                put("sorted_index", "sort sorted_index file path");
            }});
            DataQuanta filter_node = DataQuanta.createInstance("filter", new HashMap<String, String>(){{
                put("predicate", "pre-defined or user-defined function");
                put("result", "filter result file path");
            }});
            DataQuanta map_node = DataQuanta.createInstance("map", new HashMap<String, String>(){{
                put("udf", "pre-defined or user-defined function");
                put("result", "map result file path");
            }});
            DataQuanta collect_node = DataQuanta.createInstance("collect", new HashMap<String, String>(){{
                put("result", "collect result file path");
            }});

            // 链接节点，即构建DAG
            source_node.outgoing(sort_node, null);

            //test
            source_node.outgoing(collect_node,null);
            //test
            source_node.outgoing(filter_node,null);

            sort_node.outgoing(map_node, new HashMap<String, String>(){{
                put("sorted_value", "data");
            }});
            sort_node.outgoing(filter_node, new HashMap<String, String>(){{
                put("sorted_value", "data");
            }});

          /*  collect_node.incoming(map_node, new HashMap<String, String>(){{
                put("result", "data");
            }});
            collect_node.incoming(filter_node, new HashMap<String, String>(){{
                put("result", "data");
            }});*/
            filter_node.outgoing(map_node,null);
            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
