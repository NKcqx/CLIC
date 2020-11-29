package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 查看某个stage的结果（task的中间结果）
 * 目前是nfs文件，保存在本地。如果以后中间结果形式发生改变，需修改。
 *
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/12 09:31
 */
public class ShellGetStageResult {
    private static Logger logger = LoggerFactory.getLogger(ShellGetStageResult.class);

    public static void main(String[] args) {

        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        String stageId = args[0];
        int lineNum = 10;
        if (args.length == 4) {
            lineNum = Integer.parseInt(args[1]);
        }
        String resPath = taskServiceClient.getStageResult(stageId);
        if (resPath != null) {
            File file = new File(resPath);
            if (file.exists()) {
                System.out.format("\nThe result of " + stageId + " has been saved in ：" + resPath + " \n");
                try {
                    List<String> result = readFileByLine(resPath, lineNum);
                    result.forEach(System.out::println);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Error executing cat command using file path!");
                }
                logger.info("The result of " + stageId + " has been saved in ：" + resPath);
            } else {
                System.out.println("\nThe result of " + stageId + ": File path does not exist!");
                logger.info("The result of " + stageId + ": File path does not exist!");
            }

        } else {
            System.out.format("\nThe result of " + stageId + " is empty. Please check if the stage has been completed.");
            logger.info("The result of " + stageId + " is empty.");
        }
        System.out.format("\n");
    }

    /**
     * 按行读取文件
     *
     * @param path
     * @param line
     * @return
     */
    @SuppressWarnings("checkstyle:InnerAssignment")
    private static List<String> readFileByLine(String path, int line) {
        List<String> result = new ArrayList<>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(path));
            for (int i = 0; i < line; i++) {
                String str;
                if ((str = in.readLine()) != null) {
                    result.add(str);
                }
            }
        } catch (IOException e) {
            logger.error("An Error occur when readFileByLine from" + path);
            e.printStackTrace();
        }
        return result;
    }

}
