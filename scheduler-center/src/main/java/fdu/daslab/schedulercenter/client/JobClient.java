package fdu.daslab.schedulercenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.jobcenter.JobService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/16/21 10:29 AM
 */
@Component
public class JobClient extends ThriftClient<JobService.Client> {

    @Value("${thrift.job.host}")
    private String host;

    @Value("${thrift.job.port}")
    private int port;

    @Override
    protected JobService.Client createClient(TBinaryProtocol protocol) {
        return new JobService.Client(protocol);
    }

    @Override
    protected String getHost() {
        return host;
    }

    @Override
    protected int getPort() {
        return port;
    }
}
