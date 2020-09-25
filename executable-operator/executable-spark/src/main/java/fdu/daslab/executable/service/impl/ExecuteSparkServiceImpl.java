package fdu.daslab.executable.service.impl;

import base.ResultCode;
import base.ServiceBaseResult;
import base.TransParams;
import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.TopTraversal;
import fdu.daslab.executable.service.ExecuteService;
import fdu.daslab.executable.service.client.SchedulerServiceClient;
import fdu.daslab.executable.spark.operators.SparkOperatorFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * spark平台对外的运行服务的实现
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/22 3:52 PM
 */
public class ExecuteSparkServiceImpl implements ExecuteService.Iface {

    private Integer stageId;
    private String udfPath;
    private String dagPath;
    private String driverHost; // driver的thrift地址
    private Integer driverPort; // driver的thrift端口

    private static Logger logger = LoggerFactory.getLogger(ExecuteSparkServiceImpl.class);

    public ExecuteSparkServiceImpl(Integer stageId, String udfPath, String dagPath,
                                   String driverHost, Integer driverPort) {
        this.stageId = stageId;
        this.udfPath = udfPath;
        this.dagPath = dagPath;
        this.driverHost = driverHost;
        this.driverPort = driverPort;
    }

    /**
     * 实际执行workflow
     *
     * @return 本workflow返回的数据，可能需要传输给下一个节点
     * @param driverClient driver客户端
     */
    private void onExecute(SchedulerServiceClient driverClient) {
        //记录时间
        long start = System.currentTimeMillis();   //获取开始时间
        logger.info("Stage(spark) ———— Start A New Spark Stage");
        try {
            OperatorBase headOperator = ArgsUtil.parseArgs(dagPath, new SparkOperatorFactory());
            //记录输入文件的大小
            if (headOperator.getParams().containsKey("inputPath")) {
                String inputFile = (String) headOperator.getParams().get("inputPath");
                File f = new File(inputFile);
                if (f.exists() && f.isFile()) {
                    logger.info("Stage(spark) ———— Input file size:  " + f.length());
                } else {
                    logger.info("Stage(spark) ———— File doesn't exist or it is not a file");
                }
            }
            // 遍历DAG，执行execute，每次执行前把上一跳的输出结果放到下一跳的输入槽中（用Connection来转移ResultModel里的数据）
            ParamsModel inputArgs = new ParamsModel(null);
            inputArgs.setFunctionClasspath(udfPath);
            // 拓扑排序保证了opt不会出现 没得到所有输入数据就开始计算的情况
            TopTraversal topTraversal = new TopTraversal(headOperator);
            OperatorBase tailOperator = null;
            while (topTraversal.hasNextOpt()) {
                OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> curOpt = topTraversal.nextOpt();
                // 设置基本的每个operator需要和driver通信的参数
                curOpt.setDriverClient(driverClient);
                curOpt.execute(inputArgs, null);
                // 把计算结果传递到每个下一跳opt
                List<Connection> connections = curOpt.getOutputConnections(); // curOpt没法明确泛化类型
                for (Connection connection : connections) {
                    OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> targetOpt = connection.getTargetOpt();
                    String sourceKey = connection.getSourceKey();
                    String targetKey = connection.getTargetKey();
                    JavaRDD<List<String>> sourceResult = curOpt.getOutputData(sourceKey);
                    // 将当前opt的输出结果传入下一跳的输入数据
                    targetOpt.setInputData(targetKey, sourceResult);
                }
                logger.info("Stage(java) ———— Current Spark Operator is " + curOpt.getName());
                tailOperator = curOpt;
            }
            long end = System.currentTimeMillis(); //获取结束时间
            logger.info("Stage(spark) ———— Running hold time:  " + (end - start) + "ms");

            if (tailOperator != null && tailOperator.getParams().containsKey("outputPath")) {
                String outputPath = (String) tailOperator.getParams().get("outputPath");
                File f = new File(outputPath);
                if (f.exists() && f.isFile()) {
                    logger.info("Stage(spark) ———— Output file size:  " + f.length());
                } else {
                    logger.info("Stage(spark) ———— File doesn't exist or it is not a file");
                }
            }
            logger.info("Stage(spark) ———— End The Current Spark Stage");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public ServiceBaseResult execute(TransParams transParams) throws TException {
        // TODO: TransParams暂时没有用，数据交互方式目前可以直接在生成Dag的时候给定，可能有一些方式的信息只能动态生成
        SchedulerServiceClient driverClient = new SchedulerServiceClient(stageId, driverHost, driverPort);
        // TODO: 后期可以把数据的大小 / 时间上报收敛到开始结束的rpc调用上
        driverClient.postStarted();
        onExecute(driverClient);
        driverClient.postCompleted();
        return new ServiceBaseResult(ResultCode.SUCCESS, "spark execute success!");
    }
}
