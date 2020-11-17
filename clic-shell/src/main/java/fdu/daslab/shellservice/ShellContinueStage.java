package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 继续开始某个已被暂停的stage
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/12 09:30
 */
public class ShellContinueStage {
    private static Logger logger = LoggerFactory.getLogger(ShellContinueStage.class);
    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        String stageId = args[0];
        boolean isSucess = taskServiceClient.continueStage(stageId);
        if (isSucess){
            System.out.format("\n"+ stageId + " has resumed execution! \n");
            logger.info(stageId + " has resumed execution!");
        }else {
            System.out.format("\n"+ stageId + " resume execution failed! The stage may not have been suspended or other reasons!\n");
            logger.info(stageId + " resume execution failed!");
        }
        System.out.println();
    }
}
