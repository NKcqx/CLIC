package fdu.daslab.service.client;

import fdu.daslab.thrift.master.TaskService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * 和Clic-master交互的客户端，负责提交任务，查看任务状态等
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/23 6:27 PM
 */
public class TaskServiceClient {

    private TaskService.Client client;
    private TTransport transport;

    public TaskServiceClient(String masterHost, Integer masterPort) {
        this.transport = new TSocket(masterHost, masterPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol tMultiplexedProtocol = new TMultiplexedProtocol(protocol, "TaskService");
        this.client = new TaskService.Client(tMultiplexedProtocol);
    }

    /**
     * 提交任务给Clic-Master，异步
     *
     * @param planName plan名称
     * @param planDagPath plan的Dag的yaml文件的路径
     * @throws TException thrift异常
     */
    public void submitPlan(String planName, String planDagPath) throws TException {
        transport.open();
//        client.send_submitPlan(planName, planDagPath);
        client.submitPlan(planName, planDagPath);
        transport.close();
    }
}
