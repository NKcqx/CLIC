package fdu.daslab.executorcenter.executor;

import fdu.daslab.executorcenter.client.OperatorClient;
import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.base.Stage;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 在kubernetes上执行
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/23/21 5:38 PM
 */
@Component
public class KubernetesExecutor implements Executor {

    @Autowired
    private OperatorClient operatorClient;

    // 提交单个
    private void submitSinglePod(Plan plan, Platform platformInfo) {

    }

    // 提交kubernetes operator
    private void submitOperator(Plan plan, Platform platformInfo) {

    }

    @Override
    public void execute(Stage stage) throws TTransportException {
        Platform platform = null;
        operatorClient.open();
        try {
            platform = operatorClient.getClient().findPlatformInfo(stage.platformName);
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            operatorClient.close();
        }

        // 创建 pod 或者 创建 operator
        if (platform.useOperator) {
            submitSinglePod(stage.planInfo, platform);
        } else {
            submitOperator(stage.planInfo, platform);
        }

        // 保存stage的状态为已经条件

    }

    // todo: 设置watch的方式 或者 轮训的方式，去获取对应的stage的状态，stage状态更新的同时可能更新job的状态
    // stage状态更新的同时，还可能触发新的调度，如何实现？
}
