
package fdu.daslab.executable.spark;


import fdu.daslab.executable.DagExecutor;
import fdu.daslab.executable.spark.constants.SparkOperatorFactory;

/**
 * spark平台的执行器
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:55 PM
 */
public class ExecuteSparkOperator {

    public static void main(String[] args) {
        DagExecutor executor = new DagExecutor(args, new SparkOperatorFactory());
        executor.execute();
    }
}
