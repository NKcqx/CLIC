package fdu.daslab.executable.flink;

import fdu.daslab.executable.DagExecutor;
import fdu.daslab.executable.flink.constants.FlinkOperatorFactory;

/**
 * Flink平台的执行器
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/10 16:58
 */
public class ExecuteFlinkOperator {
    public static void main(String[] args) {
        DagExecutor executor = new DagExecutor(args, new FlinkOperatorFactory());
        executor.execute();
    }
}
