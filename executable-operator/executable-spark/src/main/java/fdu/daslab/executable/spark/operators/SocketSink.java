package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.basic.serialize.SerializeUtil;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 使用socket同步传输数据，仅作为示例
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/23 3:40 PM
 */
public class SocketSink extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

    public SocketSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SocketSink", id, inputKeys, outputKeys, params);
    }

    private static Logger logger = LoggerFactory.getLogger(SocketSink.class);

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        // 先发送通知告诉下一跳可以准备接收数据了，然后不同partition分别发送数据
        // TODO: 1.是否需要控制数据的发送顺序 2.是否所有数据都发送到一个节点？
        try {
            // 接下来发送数据
            ServerSocket server = new ServerSocket(Integer.parseInt(this.params.get("socketPort")));
            // 阻塞，等待客户端连接上来就发送数据
            Socket socket = server.accept();
            // 需要依次将数据序列化为字节数组，然后一个partition一个partition地发送数据
            logger.info("Start to send data to: " + socket.getRemoteSocketAddress().toString());
            // 由于socket无法被序列化，因此直接collect到一起，然后发送 TODO: 其他数据发送的选择，数据需要分批
//            List<List<String>> records = this.getInputData("data").collect();
            // 使用arrayList，而不是scala的list
            ArrayList<List<String>> records = new ArrayList<>();
            this.getInputData("data").foreachPartition(partitionIter -> {
                partitionIter.forEachRemaining(records::add);
            });
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            // 暂时只是把所有的数据序列化成一个大的字节数组
            out.write(SerializeUtil.serialize(
                    this.params.getOrDefault("serializeMethod", "java"), records));
            out.flush();
            out.close();
            socket.close();
            server.close();
            logger.info("Completed to data of sending!");
        } catch (Exception e) {
            logger.error("Spark socket sink fail: " + e.getMessage());
            // 还需要发送消息给driver，driver停止接下来的执行 / 重试
        }

    }
}
