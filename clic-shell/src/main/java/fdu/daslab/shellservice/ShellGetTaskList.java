package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import fdu.daslab.consoleprint.ConsoleTable;
import fdu.daslab.consoleprint.table.Cell;
import fdu.daslab.consoleprint.util.PrintUtil;
import fdu.daslab.utils.FieldName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 获取所有task的list
 *
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/10/22 18:14
 */
public class ShellGetTaskList {
    private static Logger logger = LoggerFactory.getLogger(ShellGetTaskList.class);

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        List<Map<String, String>> allTaskList = taskServiceClient.getTaskList();
        //如果列表不为空
        if (!allTaskList.isEmpty()) {
            List<Cell> header = new ArrayList<Cell>() {
                {
                    add(new Cell(FieldName.TASK_PLAN_NAME));
                    add(new Cell(FieldName.TASK_SUBMIT_TIME));
                    add(new Cell(FieldName.TASK_START_TIME));
                    add(new Cell(FieldName.TASK_COMPLETE_TIME));
                    add(new Cell(FieldName.TASK_STATUS));
                }
            };
            List<List<Cell>> body = new ArrayList<List<Cell>>();
            List<List<String>> taskListInfo = parseTaskList(allTaskList);
            taskListInfo.forEach(task -> {
                List<Cell> row = new ArrayList<Cell>();
                task.forEach(info -> {
                    row.add(new Cell(PrintUtil.processOutLen(info)));
                });
                body.add(row);
            });
            new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
        }
        System.out.format("\n");
        logger.info("get task list has completed!");
    }

    private static List<List<String>> parseTaskList(List<Map<String, String>> taskList) {
        List<List<String>> taskListInfo = new ArrayList<>();
        taskList.forEach(task -> {
            List<String> taskInfo = new ArrayList<>();
            taskInfo.add(task.get(FieldName.TASK_PLAN_NAME));
            taskInfo.add(task.get(FieldName.TASK_SUBMIT_TIME));
            taskInfo.add(task.get(FieldName.TASK_START_TIME));
            taskInfo.add(task.get(FieldName.TASK_COMPLETE_TIME));
            taskInfo.add(task.get(FieldName.TASK_STATUS));
            taskListInfo.add(taskInfo);
        });
        return taskListInfo;
    }
}
