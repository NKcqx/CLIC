package fdu.daslab.common.thrift;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * Thrift Client的抽象类，子类必须实现三个方法
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/19/21 10:21 AM
 */
public abstract class ThriftClient<T extends TServiceClient> {

    protected T client;
    protected TSocket transport;

    // 构造client
    protected abstract T createClient(TBinaryProtocol protocol);

    // 获取host和port
    protected abstract String getHost();
    protected abstract int getPort();

    protected void init() {
        transport = new TSocket(getHost(), getPort());
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = createClient(protocol);
    }

    public T getClient() {
        if (client == null) init();
        return client;
    }

    public void open() throws TTransportException {
        if (client == null) init();
        transport.open();
    }

    public void close() {
        transport.close();
    }


}
