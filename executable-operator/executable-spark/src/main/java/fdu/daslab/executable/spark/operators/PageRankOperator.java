package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spark Graphx的pagerank
 *
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/26 10:27
 */

public class PageRankOperator extends OperatorBase<Graph<String, String>, JavaRDD<List<String>>> {
    public PageRankOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("PageRankOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {

        Graph<String, String> graph = this.getInputData("data");
        GraphOps<String, String> graphOps = Graph.graphToGraphOps(graph, ClassTag$.MODULE$.<String>apply(String.class),
                ClassTag$.MODULE$.<String>apply(String.class));
        //resetProb:随机重置的概率,通常都是0.15，此处直接指定一下
        JavaRDD<Tuple2<Object, Object>> rankRdd = graphOps.staticPageRank(Integer.parseInt(this.params.get("iterNum")),
                0.15).vertices().toJavaRDD();
        //转换类型
        JavaRDD<List<String>> javaRDD = rankRdd.map(t -> {
            List<String> tmp = new ArrayList<>();
            tmp.add(t._1.toString());
            tmp.add(t._2.toString());
            return tmp;
        });
        this.setOutputData("result", javaRDD);

    }
}
