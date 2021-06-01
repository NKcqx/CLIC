package fdu.daslab.executorcenter.service;

import fdu.daslab.executorcenter.executor.KubernetesExecutor;
import fdu.daslab.executorcenter.executor.LocalExecutor;
import fdu.daslab.thrift.base.Stage;
import fdu.daslab.thrift.executorcenter.ExecutionResult;
import fdu.daslab.thrift.executorcenter.ExecutorService;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 执行stage
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 7:50 PM
 */
@Service
public class ExecutorHandler implements ExecutorService.Iface {

    @Value("${mode}")
    String executionMode; // 表示执行方式

    @Autowired
    private LocalExecutor localExecutor;

    @Autowired
    private KubernetesExecutor kubernetesExecutor;

    @Override
    public void executeStage(Stage stage) throws TException {
        // 执行stage，执行完成之后，需要 1.更新stage状态 2.触发后继任务的调度
        // TODO: 如何判断stage执行完成？
        if ("local".equals(executionMode)) {
            localExecutor.execute(stage);
        } else {
            kubernetesExecutor.execute(stage);
        }
    }

}
