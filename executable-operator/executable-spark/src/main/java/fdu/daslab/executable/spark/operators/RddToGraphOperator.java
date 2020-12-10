package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag$;

import java.util.List;
import java.util.Map;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/26 18:29
 */
public class RddToGraphOperator extends OperatorBase<JavaRDD<List<String>>, Graph<String, String>> {
    public RddToGraphOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("ConvertToGraphOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Graph<String, String>> result) {
        JavaRDD<List<String>> javaRdd = this.getInputData("data");
        JavaRDD<Edge<String>> edgeRdd = javaRdd.map(r -> {
            if (r.size() == 3) {
                return new Edge<String>(Long.parseLong(r.get(0)), Long.parseLong(r.get(1)), r.get(2));
            } else if (r.size() == 2) {
                return new Edge<String>(Long.parseLong(r.get(0)), Long.parseLong(r.get(1)), null);
            }
            return null;
        });
        Graph<String, String> graph = Graph.fromEdges(edgeRdd.rdd(), "", StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(), ClassTag$.MODULE$.apply(Object.class),
                ClassTag$.MODULE$.apply(Object.class));

        this.setOutputData("result", graph);
    }
}
