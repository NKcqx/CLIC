package fdu.daslab.gatewaycenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.jobcenter.JobService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author zjchenn
 * @since 2021/6/9 上午11:12
 * @description
 */

@Component
public class JobClient extends ThriftClient<JobService.Client> {

    @Value("${thrift.job.host}")
    private String host;

    @Value("${thrift.job.port}")
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
    protected JobService.Client createClient(TBinaryProtocol protocol) {
        return new JobService.Client(protocol);
    }
}
