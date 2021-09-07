package fdu.daslab.executable.hpc;

import fdu.daslab.executable.DagExecutor;
import fdu.daslab.executable.hpc.constants.HpcOperatorFactory;

/**
 * @author zjchen
 * @time 2021/9/6 10:10 上午
 * @description
 */

public class ExecuteHpcOperator {
    public static void main(String[] args) {
        DagExecutor executor = new DagExecutor(args, new HpcOperatorFactory());
        executor.execute();
    }
}
