package fdu.daslab.executable.service.client;

import fdu.daslab.executable.thrift.master.SchedulerService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;

/**
 * 调用master所需要的client
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/23 3:00 PM
 */
public class SchedulerServiceClient {

    // 调用的client
    private SchedulerService.Client client;
    private TTransport transport;
    // 当前client的ID标识
    private String stageId;

    public SchedulerServiceClient(String stageId, String masterHost, Integer masterPort) {
        this.transport = new TSocket(masterHost, masterPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        // 由于多个服务绑定一个端口，需要使用multiplex的协议
        TMultiplexedProtocol tMultiplexedProtocol = new TMultiplexedProtocol(protocol, "SchedulerService");
        this.client = new SchedulerService.Client(tMultiplexedProtocol);
        this.stageId = stageId;
    }

    // 暂时没有数据需要上报
    public void postStarted() throws TException {
        transport.open();
        client.postStageStarted(this.stageId, new HashMap<>());
        transport.close();
    }

    // 暂时没有数据需要上报
    public void postCompleted() throws TException {
        transport.open();
        client.postStageCompleted(this.stageId, new HashMap<>());
        transport.close();
    }

    // 数据准备完，上传数据的位置信息
    public void postDataPrepared() throws TException {
        transport.open();
        client.postDataPrepared(this.stageId);
        transport.close();
    }
}
