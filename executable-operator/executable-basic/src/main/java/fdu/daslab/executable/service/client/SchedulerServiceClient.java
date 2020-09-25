package fdu.daslab.executable.service.client;

import fdu.daslab.executable.service.SchedulerService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;

/**
 * driver调用的client
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
    private Integer stageId;

    public SchedulerServiceClient(Integer stageId, String driverHost, Integer driverPort) {
        this.transport = new TSocket(driverHost, driverPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        this.client = new SchedulerService.Client(protocol);
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
