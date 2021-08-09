package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.constants.SparkOperatorFactory;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
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

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
//        JavaRDD<List<String>> training_data = this.getInputData("trainingData");
//        JavaRDD<List<String>> data = this.getInputData("data");
        if (this.params.getOrDefault("fit", "false").equals("true")){
            JavaRDD<List<String>> data = this.getInputData("data");
            String vector_size = this.params.getOrDefault("vectorSize", "16");
            Word2Vec word2Vec = new Word2Vec().setMinCount(0).setVectorSize(Integer.parseInt(vector_size));
            Word2VecModel model = word2Vec.fit(data);
            scala.collection.immutable.Map<String, float[]> tmp = model.getVectors();
            Map<String, float[]> dic = scala.collection.JavaConverters
                    .mapAsJavaMapConverter(tmp).asJava();
            List<List<String>> res = new ArrayList<>();
            res.add(Arrays.asList("word", "vector"));
            for (Map.Entry<String, float[]> entry: dic.entrySet()) {
                res.add(Arrays.asList(entry.getKey(), Arrays.toString(entry.getValue())));
            }
            final JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();
            JavaRDD<List<String>> word_ebd_dict = javaSparkContext.parallelize(res);
            this.setOutputData("result", word_ebd_dict);
        }
    }
}
