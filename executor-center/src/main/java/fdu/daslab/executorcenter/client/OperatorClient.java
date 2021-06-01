package fdu.daslab.executorcenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.operatorcenter.OperatorCenter;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.stereotype.Component;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/26/21 4:55 PM
 */
@Component
public class OperatorClient extends ThriftClient<OperatorCenter.Client> {
    @Override
    protected OperatorCenter.Client createClient(TBinaryProtocol protocol) {
        return null;
    }

    @Override
    protected String getHost() {
        return null;
    }

    @Override
    protected int getPort() {
        return 0;
    }
}
