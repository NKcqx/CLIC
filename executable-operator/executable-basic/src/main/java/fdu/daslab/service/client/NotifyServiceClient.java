package fdu.daslab.service.client;

import fdu.daslab.thrift.notifyservice.NotifyService;
import fdu.daslab.thrift.notifyservice.StageSnapshot;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现对于任务的通知机制
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/9/21 4:47 PM
 */
public class NotifyServiceClient {

    private Logger logger = LoggerFactory.getLogger(NotifyServiceClient.class);

    // 调用的client
    private NotifyService.Client client;
    private TTransport transport;
    // 当前client的ID标识
    private int stageId;
    // 当前的jobName
    private String jobName;
    // 标记是否是debug模式，不尝试和master连接
    private boolean isDebug = false;

    public NotifyServiceClient(int stageId, String jobName, String host, int port) {
        // 如果参数为空，推断出是本地模式，不会和master建立thrift连接
        if (host == null && port == 0) {
            isDebug = true;
        } else {
            this.transport = new TSocket(host, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            // 由于多个服务绑定一个端口，需要使用multiplex的协议
            TMultiplexedProtocol tMultiplexedProtocol = new TMultiplexedProtocol(protocol, "NotifyService");
            this.client = new NotifyService.Client(tMultiplexedProtocol);
            this.stageId = stageId;
            this.jobName = jobName;
        }
    }

    /**
     * 向上游上传个人信息
     *
     * @param snapshot stage的信息
     */
    public void notify(StageSnapshot snapshot) {
        if (isDebug) {
            logger.info("debug info: {}", snapshot.message);
            return;
        }
        try {
            transport.open();
            client.postStatus(jobName, stageId, snapshot);
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }
}
