package fdu.daslab.optimizercenter.repository;

import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.optimizercenter.OptimizerModel;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 4:40 PM
 */
@Repository
public class OptimizerRepository {

    private List<OptimizerModel> optimizerModels = new ArrayList<>();

    // 注册优化器，先简单，保存到内存
    public void registerOptimizer(OptimizerModel optimizerModel) {
        optimizerModels.add(optimizerModel);
    }

    // 查找满足当前计划的所有优化器
    public List<OptimizerModel> filterPossibleOptimizers(Plan plan) {
        return optimizerModels;
    }
}
