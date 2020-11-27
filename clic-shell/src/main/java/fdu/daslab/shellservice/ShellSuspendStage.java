package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 暂停某个未开始被执行的stage
 *
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/12 09:29
 */
public class ShellSuspendStage {
    private static Logger logger = LoggerFactory.getLogger(ShellSuspendStage.class);

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        String stageId = args[0];
        boolean isSucess = taskServiceClient.suspendStage(stageId);
        if (isSucess) {
            System.out.format("\n" + stageId + " has been suspended! \n");
            logger.info(stageId + " has been suspended!");
        } else {
            System.out.format("\n" + stageId + " suspend failed!The stage may have started or completed!\n");
            logger.info(stageId + " suspend failed!");
        }
        System.out.format("\n");
    }
}
