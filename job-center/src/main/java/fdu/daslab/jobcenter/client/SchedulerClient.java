package fdu.daslab.jobcenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.schedulercenter.SchedulerService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 3:48 PM
 */
@Component
public class SchedulerClient extends ThriftClient<SchedulerService.Client> {

    @Value("${thrift.scheduler.host}")
    private String host;

    @Value("${thrift.scheduler.port}")
    private int port;

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    protected SchedulerService.Client createClient(TBinaryProtocol protocol) {
        return new SchedulerService.Client(protocol);
    }

}
