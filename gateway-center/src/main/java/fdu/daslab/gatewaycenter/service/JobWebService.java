package fdu.daslab.gatewaycenter.service;

import fdu.daslab.gatewaycenter.client.JobClient;
import fdu.daslab.gatewaycenter.utils.PlanBuilder;
import fdu.daslab.thrift.base.Plan;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author zjchenn
 * @since 2021/6/10 上午10:06
 * @description
 */

@Service
public class JobWebService {

    @Autowired
    public JobClient jobService;

    @Autowired
    public PlanBuilder planBuilder;

    public void submit(String jobName, String planJsonString) throws TException {
        Plan plan = planBuilder.parseJson(planJsonString);
        jobService.open();
        try {
            jobService.getClient().submit(plan, jobName);
        } finally {
            jobService.close();
        }
    }
}
