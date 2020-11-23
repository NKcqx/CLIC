package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.basic.serialize.SerializeUtil;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * spark socket source很奇怪，来源应该至少也是一个分布式的平台 ？
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/23 3:40 PM
 */
public class SocketSource extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

    public SocketSource(String id, List<String> inputKeys, List<String>
            outputKeys, Map<String, String> params) {
        super("SocketSource", id, inputKeys, outputKeys, params);
    }

    private static Logger logger = LoggerFactory.getLogger(SocketSource.class);

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        try {
            try (Socket socket = new Socket(this.params.get("socketHost"),
                    Integer.parseInt(this.params.get("socketPort")))) {
                InputStream inputStream = socket.getInputStream();
                byte[] bytes = new byte[10240]; // 一次只能接收1024个字节，后面再调整其他方式
                List<List<String>> resultList = new ArrayList<>();

                logger.info("Start to accept data from: " + socket.getRemoteSocketAddress().toString());
                // 默认使用原生java序列化方式
                int len = 0;
                while ((len = inputStream.read(bytes)) != -1) {
                    @SuppressWarnings("unchecked")
                    List<List<String>> cur = (List<List<String>>) SerializeUtil.deserialize(
                            this.params.getOrDefault("serializeMethod", "java"), bytes, len);
                    resultList.addAll(cur);
                }
                final JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();
                this.setOutputData("result", javaSparkContext.parallelize(resultList));
                inputStream.close();
            }
            logger.info("Complete data accepting.");

        } catch (Exception e) {
            e.printStackTrace();
            // 最好还需要向driver发送消息
        }
    }
}
