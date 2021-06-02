package fdu.daslab.schedulercenter.scheduling;

import fdu.daslab.schedulercenter.client.ExecutorClient;
import fdu.daslab.schedulercenter.client.SortPluginClient;
import fdu.daslab.schedulercenter.repository.SchedulerRepository;
import fdu.daslab.thrift.base.Stage;
import fdu.daslab.thrift.executorcenter.ExecutorService;
import fdu.daslab.thrift.schedulercenter.PluginType;
import fdu.daslab.thrift.schedulercenter.SchedulerModel;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * stage的调度
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/1/21 7:31 PM
 */
@Component
public class StageScheduling {

    @Autowired
    private SchedulerRepository schedulerRepository;

    @Autowired
    private ExecutorClient executorClient;

    // 没有完成还在等待执行的stage
    private List<Stage> pendingStages = new ArrayList<>();

    // 调度逻辑：触发动作主要有两块，一个是新的stage过来，一个是老的stage执行完成
    public void schedule(List<Stage> stageList) throws TException {
        // 提交给scheduler plugin执行调度
        // 优化器可以有多层，但是调度器应该是固定的（互斥），应该类似Kubernetes，实现不同的拓展点
        // 这里需要考虑多个拓展点的不同实现
        final Optional<SchedulerModel> sortModel = schedulerRepository.findAllScheduler().stream()
                .filter(scheduler -> scheduler.pluginType.equals(PluginType.SORT_PLUGIN))
                .max(Comparator.comparingInt(scheduler -> scheduler.priority));
        pendingStages.addAll(stageList);
        if (sortModel.isPresent()) {
            SortPluginClient client = new SortPluginClient(sortModel.get());
            try {
                client.open();
                pendingStages = client.getClient().sort(stageList);
            } catch (TException e) {
                e.printStackTrace();
            } finally {
                client.close();
            }
        }
        // 执行pendingStages
        for (Stage stage : pendingStages) {
            executorClient.open();
            try {
                executorClient.getClient().executeStage(stage);
            } finally {
                executorClient.close();
            }
        }
    }
}
