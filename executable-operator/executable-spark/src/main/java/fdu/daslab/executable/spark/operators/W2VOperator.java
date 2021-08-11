package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.constants.SparkOperatorFactory;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.glassfish.jersey.message.internal.StringBuilderUtils;
import org.javatuples.Pair;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2021/8/6 下午7:11
 */
public class W2VOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {
    public W2VOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkW2VOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        if (this.params.getOrDefault("fit", "false").equals("true")){
            JavaRDD<List<String>> data = this.getInputData("data");
            String vector_size = this.params.getOrDefault("vectorSize", "16");
            Word2Vec word2Vec = new Word2Vec().setMinCount(0).setVectorSize(Integer.parseInt(vector_size));
            Word2VecModel model = word2Vec.fit(data);

            List<List<String>> res = new CopyOnWriteArrayList<>();
            scala.collection.immutable.Map<String, float[]> tmp = model.getVectors();
            scala.collection.Iterator<Tuple2<String, float[]>> it = tmp.iterator();
            while (it.hasNext()){
                Tuple2<String, float[]> row = it.next();
//                row._2().toString() StringUtils.join(ArrayUtils.toObject(row._2()), ",")
                List<String> line = Collections.synchronizedList(
                        Arrays.asList(
                                row._1(), "\"["+StringUtils.join(ArrayUtils.toObject(row._2()), ",")+"]\""
                        )
                );
                res.add(line);
            }

            final JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();
            JavaRDD<List<String>> word_ebd_dict = javaSparkContext.parallelize(res);
            this.setOutputData("result", word_ebd_dict);
        }
    }

    public static void main(String[] args) {
        JavaSparkContext javaSparkContext;
        FileSource source = null;
        W2VOperator w2v = null;
        FileSink sink = null;
        SparkOperatorFactory sparkOperatorFactory = new SparkOperatorFactory();

        HashMap<String, String> params = new HashMap<>();
        params.put("inputPath", "/Users/jason/Desktop/Spark-w2v-senti/ptb/ptb.train.txt");
        params.put("separator", " ");
        try{
            source = (FileSource) sparkOperatorFactory.createOperator(
                    "SourceOperator",
                    "1",
                    Collections.singletonList("data"),
                    Collections.singletonList("result"),
                    params);
            HashMap<String, String> params2 = new HashMap<>();
            params2.put("vectorSize", "16");
            params2.put("fit", "true");
            w2v = (W2VOperator) sparkOperatorFactory.createOperator(
                    "W2VOperator",
                    "2",
                    Collections.singletonList("data"),
                    Collections.singletonList("result"),
                    params2
            );

            HashMap<String, String> params3 = new HashMap<>();
            params3.put("outputPath", "/Users/jason/Desktop/Spark-w2v-senti/output/w2v_output.csv");
            params3.put("separator", ",");
            sink  = (FileSink) sparkOperatorFactory.createOperator(
                    "SinkOperator",
                    "3",
                    Collections.singletonList("data"),
                    Collections.singletonList("result"),
                    params3
            );
            source.connectTo("result", w2v, "data");
            w2v.connectTo("result", sink, "data");
        }catch (Exception e){
            e.printStackTrace();
        }

        SparkInitUtil.setSparkContext(
                new SparkConf().setMaster("local[*]").setAppName("W2VOperatorTest"));
        javaSparkContext = SparkInitUtil.getDefaultSparkContext();

        try {
            // 不能使用之前的BFSTraversal
            Queue<OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>>> bfsQueue = new LinkedList<>();
            bfsQueue.add(source);
            while (!bfsQueue.isEmpty()) {
                OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> curOpt = bfsQueue.poll();
                curOpt.execute(null, null);

                List<Connection> connections = curOpt.getOutputConnections(); // curOpt没法明确泛化类型
                for (Connection connection : connections) {
                    OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> targetOpt = connection.getTargetOpt();
                    bfsQueue.add(targetOpt);

                    List<Pair<String, String>> keyPairs = connection.getKeys();
                    for (Pair<String, String> keyPair : keyPairs) {
                        JavaRDD<List<String>> sourceResult = curOpt.getOutputData(keyPair.getValue0());
                        // 将当前opt的输出结果传入下一跳的输入数据
                        targetOpt.setInputData(keyPair.getValue1(), sourceResult);
                    }
                }
            }
            List<List<String>> result = w2v.getOutputData("result").collect() ;
        } catch (Exception ignored) {
        }
    }
}
