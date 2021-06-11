package fdu.daslab.executorcenter.executor;

import fdu.daslab.thrift.base.Stage;
import org.springframework.stereotype.Component;

/**
 * 本地执行器，为了方便策略，TODO: 后面实现
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/23/21 5:37 PM
 */
@Component
public class LocalExecutor implements Executor {
    @Override
    public void execute(Stage stage) {

    }
}
