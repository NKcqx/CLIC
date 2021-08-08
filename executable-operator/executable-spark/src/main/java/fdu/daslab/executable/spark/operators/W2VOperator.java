package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.sparkproject.guava.collect.Lists;

import java.util.*;
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

    public static void main(String[] args) {
//        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        final JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("default"));
        // FileSource sourceArgs = (FileSource) inputArgs.getOperatorParam();
        // 读取文件，并按照分割符分隔开来
        final JavaRDD<List<String>> data = javaSparkContext
                .textFile("/Users/jason/Desktop/Spark-w2v-senti/ptb/ptb.train.txt", javaSparkContext.defaultParallelism())
                .map(line -> Arrays.asList(line.split(" ")));
        data.persist(StorageLevel.MEMORY_AND_DISK());
        Word2Vec word2Vec = new Word2Vec().setMinCount(0);
        Word2VecModel model = word2Vec.fit(data);
//        model.save(SparkContext.getOrCreate(), "/Users/jason/Desktop/output");
        final JavaRDD<List<String>> nextStream = data.map(d -> d.stream()
                .map(word -> {
                            Vector v = model.transform(word);
                            double[] res = v.toArray();
                            return Arrays.toString(res);
                        })
                .collect(Collectors.toList()));
        System.out.println("================= After ==================");
        final List<List<String>> nextStream2 = nextStream.sample(false, 0.001).collect();
        for (List<String> line : nextStream2){
            System.out.println(line.toString());
        }
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        JavaRDD<List<String>> training_data = this.getInputData("trainingData");
        JavaRDD<List<String>> data = this.getInputData("data");

        if (training_data == null){
            training_data = data;
        }
        String vector_size = this.params.getOrDefault("vectorSize", "16");
        Word2Vec word2Vec = new Word2Vec().setMinCount(0).setVectorSize(Integer.parseInt(vector_size));
        Word2VecModel model = word2Vec.fit(training_data);
        final JavaRDD<List<String>> nextStream = data.map(d -> d.stream()
                .map(word -> {
                    Vector v = model.transform(word);
                    double[] res = v.toArray();
                    return Arrays.toString(res);
                })
                .collect(Collectors.toList()));
        this.setOutputData("result", nextStream);
    }
}
