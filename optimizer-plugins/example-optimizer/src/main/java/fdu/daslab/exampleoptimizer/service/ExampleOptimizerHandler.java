package fdu.daslab.exampleoptimizer.service;

import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.optimizercenter.OptimizerPlugin;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;


/**
 * example optimizer
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 3:03 PM
 */
@Service
public class ExampleOptimizerHandler implements OptimizerPlugin.Iface {

    // 这里实现优化的逻辑即可，这里不实现任何优化
    @Override
    public Plan optimize(Plan plan, Map<String, Platform> platforms) throws TException {
        return plan;
    }
}
