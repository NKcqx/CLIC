package fdu.daslab.executable.spark.operators.streaming;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * streaming的source算子
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class StreamSource extends OperatorBase<JavaDStream<List<String>>, JavaDStream<List<String>>> {

    public StreamSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaDStream<List<String>>> result) {
        // 获取streaming模式上下文
        final JavaStreamingContext streamingContext = SparkInitUtil.getDefaultStreamingContext();

        JavaDStream<List<String>> inputStream = null;

        // 获取读取数据源的方式
        String kind = this.params.get("kind");
        String separator = this.params.get("separator");

        switch (kind) {
            case "socket": // 数据流来自网络socket
                String ip = this.params.get("ip");
                if (ip == null) {
                    throw new IllegalArgumentException("ip address is wrong");
                }
                int port = Integer.parseInt(this.params.get("port"));
                if (port == 0) {
                    throw new IllegalArgumentException("port is wrong");
                }
                inputStream = streamingContext.socketTextStream(ip, port)
                        .map(data -> {
                            return Arrays.asList(data.split(separator));
                        });
                break;
            case "file": // 数据流来自文件
                String inputPath = this.params.get("inputPath");
                inputStream = streamingContext.textFileStream(inputPath)
                .map(data -> {
                    return Arrays.asList(data.split(separator));
                });
                break;
            default:
                ;
        }

        this.setOutputData("result", inputStream);
    }
}

