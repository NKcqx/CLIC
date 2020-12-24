
package fdu.daslab.executable.java;

import fdu.daslab.executable.DagExecutor;
import fdu.daslab.executable.java.constants.JavaOperatorFactory;

/**
 * java平台的执行器
 *
 * @author 唐志伟，陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:48 PM
 */
public class ExecuteJavaOperator {

    /*
        这里可以定义一些平台特殊的参数，只是为了向用户说明，最终数据会存在一个HashMap中
        例子：
            @Parameter("--Dparam")
            String param;
        最终会保存在一个HashMap中，key=param
     */

    public static void main(String[] args) {
        DagExecutor executor = new DagExecutor(args, new JavaOperatorFactory());
        executor.execute();

        /*
            有些情况下，需要定义一些平台的preHandler, postHandler方法
            public class JavaHook {
                @Override
                public void preHandler(Map<String, String> platformArgs) {
                    logger.info("pre param:", platformArgs.get("param");
                }

                @Override
                public void postHandler(Map<String, String> platformArgs) {
                    logger.info("post param:", platformArgs.get("param");
                }
            }
            DagExecutor executor = new DagExecutor(args, new JavaOperatorFactory(), new JavaHook());
            executor.execute();
         */
    }
}
