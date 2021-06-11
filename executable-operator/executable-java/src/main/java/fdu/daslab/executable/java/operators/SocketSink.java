package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.basic.serialize.SerializeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * java平台sink source，通过socket写入数据到另一端
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/25 10:04 AM
 */
public class SocketSink extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    private static Logger logger = LoggerFactory.getLogger(SocketSink.class);

    public SocketSink(String id, List<String> inputKeys,
                      List<String> outputKeys, Map<String, String> params) {
        super("SocketSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        try {
            // 接下来发送数据
            ServerSocket server = new ServerSocket(Integer.parseInt(this.params.get("socketPort")));
            // 阻塞，等待客户端连接上来就发送数据
            Socket socket = server.accept();
            logger.info("Start to send data to: " + socket.getRemoteSocketAddress().toString());
            // 需要依次将数据序列化为字节数组，然后直接所有数据一起发送给client
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            // 一个partition的数据一起序列化，一起发送
            List<List<String>> records = this.getInputData("data").collect(Collectors.toList());
            // 默认使用java原生的序列化方法
            out.write(SerializeUtil.serialize(
                    this.params.getOrDefault("serializeMethod", "java"), records));
            logger.info("written byte size: " + out.size());
            out.flush();
            out.close();
            socket.close();
            server.close();
            logger.info("Completed to data of sending!");
        } catch (Exception e) {
            logger.error("Java socket sink fail: " + e.getMessage());
            // 还需要发送消息给driver，driver停止接下来的执行 / 重试
        }
    }
}
