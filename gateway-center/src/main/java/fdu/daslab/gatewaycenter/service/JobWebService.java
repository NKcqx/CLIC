package fdu.daslab.gatewaycenter.service;

import fdu.daslab.gatewaycenter.client.JobClient;
import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.jobcenter.JobService;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author zjchenn
 * @since 2021/6/10 上午10:06
 * @description
 */
@Component
public class JobWebService {

    @Autowired
    private JobClient jobService;

    public void submit(Plan plan, String jobName) throws TException {

        JobService.Client client = jobService.getClient();
        client.submit(plan, jobName);

    }

}
