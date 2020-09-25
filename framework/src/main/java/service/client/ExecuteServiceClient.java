package service.client;

import base.ServiceBaseResult;
import base.TransParams;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import service.ExecuteService;

/**
 * 各个stage调用的客户端
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/25 2:37 PM
 */
public class ExecuteServiceClient {

    private ExecuteService.Client client;
    private TTransport transport;

    public ExecuteServiceClient(String stageHost, Integer stagePort) {
        this.transport = new TSocket(stageHost, stagePort);
        TProtocol protocol = new TBinaryProtocol(transport);
        this.client = new ExecuteService.Client(protocol);
    }

    // 执行client
    public ServiceBaseResult executeStage(TransParams params) throws TException {
        transport.open();
        ServiceBaseResult result = client.execute(params);
        transport.close();
        return result;
    }
}
