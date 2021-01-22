package fdu.daslab.client;

import fdu.daslab.thrift.master.TaskService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 与 task service连接获取信息的client
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/10/22 16:47
 */
public class TaskServiceClient {
    private static Logger logger = LoggerFactory.getLogger(TaskServiceClient.class);
    private TaskService.Client client;
    private TTransport transport;

    /**
     * @param masterHost    driver的hostip信息
     * @param masterPort    driver的port信息
     */
    public TaskServiceClient(String masterHost, Integer masterPort) {
        this.transport = new TSocket(masterHost, masterPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol tMultiplexedProtocol = new TMultiplexedProtocol(protocol, "TaskService");
        this.client = new TaskService.Client(tMultiplexedProtocol);

    }

    /**
     * 获取所有task列表信息
     * @return tasklistmap
     */
    public List<Map<String, String>> getTaskList() {
        List<Map<String, String>>  allTaskList = new ArrayList<>();
        try {
            transport.open();
            allTaskList = client.listAllTask();
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell get taskList");
            e.printStackTrace();
        }
        return  allTaskList;
    }

    /**
     * 提交任务，异步
     *
     * @param planName plan名称
     * @param planDagPath plan的Dag的yaml文件的路径
     * @throws TException thrift异常
     */
    public void submitPlan(String planName, String planDagPath) {
        try {
            transport.open();
            client.submitPlan(planName, planDagPath);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell submit plan");
            e.printStackTrace();
        }
    }

    /**
     * 根据plan name获取task/plan的信息
     *
     * @param planName task name
     * @return taskInfo
     */
    public Map<String, String> getTaskInfo(String planName) {
        Map<String, String>  taskInfo = new HashMap<>();
        try {
            transport.open();
            taskInfo = client.getTaskInfo(planName);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell get taskInfo by plan name");
            e.printStackTrace();
        }
        return  taskInfo;
    }
    /**
     * 根据plan name获取task下的所有stage id信息。
     *
     * @param planName task name
     * @return stageIdList
     */
    public List<String> getStageIdOfTask(String planName) {
        List<String>  stageIdList = new ArrayList<>();
        try {
            transport.open();
            stageIdList = client.getStageIdOfTask(planName);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell get stageIdList by plan name");
            e.printStackTrace();
        }
        return  stageIdList;
    }
    /**
     * 根据stage id获取stage的信息
     *
     * @param stageId  stage id
     * @return tasklistmap
     */
    public Map<String, String> getStageInfo(String stageId) {
        Map<String, String>  stageInfo = new HashMap<>();
        try {
            transport.open();
            stageInfo = client.getStageInfo(stageId);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell get stage info by stage Id");
            e.printStackTrace();
        }
        return  stageInfo;
    }

    public boolean suspendStage(String stageId) {
        boolean isSucess = false;
        try {
            transport.open();
            isSucess = client.suspendStage(stageId);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell suspendStage by stage Id");
            e.printStackTrace();
        }
        return isSucess;
    }

    public boolean continueStage(String stageId) {
        boolean isSucess = false;
        try {
            transport.open();
            isSucess = client.continueStage(stageId);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell continueStage by stage Id");
            e.printStackTrace();
        }
        return isSucess;
    }

    public String getStageResult(String stageId) {
        String stageResPath = null;
        try {
            transport.open();
            stageResPath = client.getStageResult(stageId);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell getStageResult by stage Id");
            e.printStackTrace();
        }
        return stageResPath;
    }



}
