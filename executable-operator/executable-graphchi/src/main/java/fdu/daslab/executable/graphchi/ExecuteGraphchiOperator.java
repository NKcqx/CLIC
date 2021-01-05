package fdu.daslab.executable.graphchi;

import fdu.daslab.executable.DagExecutor;
import fdu.daslab.executable.graphchi.constants.GraphchiOperatorFactory;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/23 16:06
 */

public class ExecuteGraphchiOperator {

    public static void main(String[] args) {
        DagExecutor executor = new DagExecutor(args, new GraphchiOperatorFactory());
        executor.execute();
    }

}
