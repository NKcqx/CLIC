package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 根据 planName和planDagPath提交一个task任务
 *
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/04 17:44
 */
public class ShellSubmitPlan {
    private static Logger logger = LoggerFactory.getLogger(ShellSubmitPlan.class);

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        if (size == 4) {
            String masterHost = args[size - 2];
            Integer masterPort = Integer.parseInt(args[size - 1]);
            TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
            String planName = args[0];
            String planDagPath = args[1];
            if (checkout(planName, planDagPath)) {
                taskServiceClient.submitPlan(planName, planDagPath);
                System.out.format(planName + " has been submitted!\n");
            }
            logger.info(planName + " has been submitted!");
        } else {
            System.out.format("Parameter error!\n");
            logger.info("Parameter error!");
        }
        System.out.format("\n");


    }

    private static boolean checkout(String planName, String planDagPath) {
        if (planName.contains("_")) {
            System.out.format("PlanName are not allowed to contain \"_\", please check it and retry!\n");
            return false;
        }
        File file = new File(planDagPath);
        if (!file.exists()) {
            System.out.format("The yaml file is not found, please check and retry!\n");
            return false;
        }
        System.out.format("Notice: If planName contains uppercase letters, it will be converted to lowercase letters\n");
        return true;
    }
}
