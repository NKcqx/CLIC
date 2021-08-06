package fdu.daslab.schedulercenter.repository;

import fdu.daslab.thrift.schedulercenter.SchedulerModel;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * 对 调度器 进行增删查改
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/24/21 4:21 PM
 */
@Repository
public class SchedulerRepository {

    private List<SchedulerModel> schedulerModels = new ArrayList<>();

    public void saveScheduler(SchedulerModel model) {
        schedulerModels.add(model);
    }

    public List<SchedulerModel> findAllScheduler() {
        return schedulerModels;
    }

}
