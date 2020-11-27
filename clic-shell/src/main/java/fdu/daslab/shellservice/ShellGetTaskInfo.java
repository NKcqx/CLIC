package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import fdu.daslab.consoleprint.ConsoleTable;
import fdu.daslab.consoleprint.table.Cell;
import fdu.daslab.consoleprint.util.PrintUtil;
import fdu.daslab.utils.FieldName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * 根据task name获取具体task的详细信息
 *
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/05 09:37
 */
public class ShellGetTaskInfo {
    private static Logger logger = LoggerFactory.getLogger(ShellGetTaskInfo.class);

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        String planName = args[0];
        Map<String, String> taskInfo = taskServiceClient.getTaskInfo(planName);

        //如果stageinfo不为空
        if (!taskInfo.isEmpty()) {
            List<Cell> header = new ArrayList<Cell>() {
                {
                    add(new Cell(FieldName.TASK_PLAN_NAME));
                    add(new Cell(FieldName.TASK_SUBMIT_TIME));
                    add(new Cell(FieldName.TASK_START_TIME));
                    add(new Cell(FieldName.TASK_COMPLETE_TIME));
                    add(new Cell(FieldName.TASK_STATUS));
                    add(new Cell(FieldName.TASK_STAGE_NUM));
                    add(new Cell(FieldName.TASK_STAGE_LIST));
                }
            };
            List<List<Cell>> body = new ArrayList<List<Cell>>();
            List<String> stageId = stringToList(taskInfo.get(FieldName.TASK_STAGE_LIST));
            for (int i = 0; i < stageId.size(); i++) {
                List<Cell> row = new ArrayList<Cell>();
                if (i == 0) {
                    row.add(new Cell(PrintUtil.processOutLen(taskInfo.get(FieldName.TASK_PLAN_NAME))));
                    row.add(new Cell(PrintUtil.processOutLen(taskInfo.get(FieldName.TASK_SUBMIT_TIME))));
                    row.add(new Cell(PrintUtil.processOutLen(taskInfo.get(FieldName.TASK_START_TIME))));
                    row.add(new Cell(PrintUtil.processOutLen(taskInfo.get(FieldName.TASK_COMPLETE_TIME))));
                    row.add(new Cell(PrintUtil.processOutLen(taskInfo.get(FieldName.TASK_STATUS))));
                    row.add(new Cell(PrintUtil.processOutLen(taskInfo.get(FieldName.TASK_STAGE_NUM))));
                } else {
                    for (int j = 0; j < 6; j++) {
                        row.add(new Cell(""));
                    }
                }
                row.add(new Cell(stageId.get(i)));
                body.add(row);
            };
            new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
        }
        System.out.format("\n");
        logger.info("get task info has completed!");
    }

    private static List<String> stringToList(String strs) {
        String str[] = strs.split(",");
        return Arrays.asList(str);
    }

}
