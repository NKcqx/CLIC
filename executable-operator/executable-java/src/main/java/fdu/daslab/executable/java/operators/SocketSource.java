package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.basic.serialize.SerializeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 *  java平台socket source，启动后直接从远程的路径下读取数据到stream中
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/23 7:49 PM
 */
public class SocketSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    public SocketSource(String id, List<String> inputKeys, List<String>
            outputKeys, Map<String, String> params) {
        super("SocketSource", id, inputKeys, outputKeys, params);
    }

    private static Logger logger = LoggerFactory.getLogger(SocketSource.class);

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        try (Socket socket = new Socket(this.params.get("socketHost"),
                Integer.parseInt(this.params.get("socketPort")))) {
            InputStream inputStream = socket.getInputStream();
            byte[] bytes = new byte[10240]; // 一次只能接收10240个字节，后面再调整其他方式
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
            this.setOutputData("result", resultList.stream());
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 防止出现异常socket未关闭
            logger.info("Complete data accepting.");
        }
    }
}
