package fdu.daslab.executable.spark.operators.streaming;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 测试算子，暂不删除
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class StreamShopping extends OperatorBase<JavaDStream<List<String>>, JavaDStream<List<String>>> {

    public StreamShopping(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaDStream<List<String>>> result) {
        // 获取streaming模式上下文
        final JavaStreamingContext streamingContext = SparkInitUtil.getDefaultStreamingContext();

        JavaDStream<List<String>> list = this.getInputData("data");
        // 按行为过滤
        JavaDStream<List<String>> filter = list.filter(line -> {
            if (line.get(1).equals("view")) {
                return true;
            } else {
                return false;
            }
        });
        JavaPairDStream<String, Integer> gidPairs = filter.mapToPair(line -> {
            String gid = line.get(2);
            return new Tuple2<>(gid, 1);
        });
        JavaPairDStream<String, Integer> gidReduce =
                gidPairs.reduceByKey(
                        (i1, i2) -> i1 + i2
                );
        JavaPairDStream<String, Integer> gidWReduce = gidReduce.window(
                Durations.seconds(9), Durations.seconds(3)
        );
        gidWReduce.foreachRDD(
                rdd -> {
                    List<Tuple2<String, Integer>> results = rdd.sortByKey().collect();
                    results.forEach(data -> System.out.println(data._1+"-"+data._2));
                }
        );

        // 流处理在这一步才会触发执行
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
