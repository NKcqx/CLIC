package fdu.daslab.executable.spark.operators.streaming;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * streaming的sort算子
 * spark streaming的sort必须要将流转成rdd后才能用
 * 相当于sink算子
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class StreamSort extends OperatorBase<JavaPairDStream<String, Integer>, JavaPairDStream<String, Integer>> {

    public StreamSort(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamSort", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaPairDStream<String, Integer>> result) {
        // 获取streaming模式上下文
        final JavaStreamingContext streamingContext = SparkInitUtil.getDefaultStreamingContext();
        // 开始流处理
        JavaPairDStream<String, Integer> inputStream = this.getInputData("data");

        // 先判断有没有topN需求
        Integer topNum = Integer.parseInt(this.params.get("topNum"));

        if (topNum == -1) {
            inputStream.foreachRDD(
                    rdd -> {
                        List<Tuple2<String, Integer>> results = rdd.sortByKey().collect();
                        results.forEach(data -> System.out.println(data._1+"-"+data._2));
                    }
            );
        } else {
            inputStream.foreachRDD(
                    rdd -> {
                        List<Tuple2<String, Integer>> results = rdd.sortByKey().take(topNum);
                        results.forEach(data -> System.out.println(data._1+"-"+data._2));
                    }
            );
        }

        // 流处理在这一步才会触发执行
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
