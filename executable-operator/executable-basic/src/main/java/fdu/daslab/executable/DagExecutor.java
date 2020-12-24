package fdu.daslab.executable;

import com.beust.jcommander.JCommander;
import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.TopoTraversal;
import fdu.daslab.service.client.SchedulerServiceClient;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 *
 * 所有java平台需要的执行器，传入参数 和 解析好的dag，以及operator的实现逻辑，可直接实现执行
 *
 * 理想情况：引入新平台(java)，只需要完成以下工作
 *  1.定义好平台可能需要的固有参数
 *  2.继承/实现接口，去实现operator，operator的形式应该是: resultType operator(dependencyResults, operatorArgs)
 *  3.构造一个入口，实现以下形式：
 *  // 准备好的参数
 *  args =
 *  // operator的factory
 *  factory =
 *  // 构造DagExecutor
 *  executor = new DagExecutor(args, factory);
 *  // 执行，可以选择同步还是异步
 *  executor.execute();
 *
 *  <b>以后我们只需要维护各个语言的core，引入新的平台做的工作只是实现对应的operator而已</b>
 *
 * @author 唐志伟
 * @version 1.0
 * @since 12/22/20 10:11 AM
 */

public class DagExecutor {

    private Logger logger = LoggerFactory.getLogger(DagExecutor.class);

    // 平台的独有参数
    private Map<String, String> platformArgs;

    // 每个平台的共有参数，比如dagPath等
    private DagArgs basicArgs;

    // 当前dag的operator的头节点 TODO: 系统中包含大量范型和raw类型的滥用，有时间逐步规范化
    private List<OperatorBase> headOperators;

    // 一些平台特殊的处理逻辑
    private DagHook hook = new DagHook();

    // 连接master所需要的客户端
    private SchedulerServiceClient masterClient = null;

    // 初始化参数
    private void initArgs(String[] args) {
        basicArgs = new DagArgs();
        // 解析公有参数
        JCommander.newBuilder()
                .addObject(basicArgs)
                .build()
                .parse(args);
        // 解析平台独有的参数
        platformArgs = basicArgs.platformArgs;
    }

    // 初始化执行的operator
    private void initOperators(OperatorFactory factory) {
        try {
            InputStream yamlStream = new FileInputStream(new File(basicArgs.dagPath));
            Pair<List<OperatorBase>, List<OperatorBase>> headAndEndOperators =
                    ArgsUtil.parseArgs(yamlStream, factory);
            headOperators = headAndEndOperators.getValue0();
        } catch (Exception e) {
            // TODO: 错误需要上报，并直接中止结束
            logger.error(e.getMessage());
        }
    }

    // 初始化thrift client
    private void initMasterClient() {
        // 创建一个thrift client，用于和master进行交互
        masterClient = new SchedulerServiceClient(basicArgs.stageId,
                basicArgs.masterHost, basicArgs.masterPort);
    }

    /**
     * 实现初始化逻辑
     *
     * @param args 命令行的参数
     * @param factory 所有operator的工厂类
     */
    public DagExecutor(String[] args, OperatorFactory factory) {
        // 解析参数，分别获取basicArgs和platformArgs
        initArgs(args);
        // 初始化master的客户端
        initMasterClient();
        // 读取dag文件，解析生成所有的operator的列表
        initOperators(factory);
    }

    /**
     * 初始化逻辑，平台可以实现一些preHandler， postHandler方法
     *
     * @param args 命令行的参数
     * @param factory 所有operator的工厂类
     * @param dagHook 平台实现的方法
     */
    public DagExecutor(String[] args, OperatorFactory factory, DagHook dagHook) {
        this(args, factory);
        hook = dagHook;
    }

    /**
     * Dag的执行逻辑
     */
    private void executeDag() {
        // 遍历DAG，执行execute，每次执行前把上一跳的输出结果放到下一跳的输入槽中（用Connection来转移ResultModel里的数据）
        ParamsModel inputArgs = new ParamsModel(null);
        inputArgs.setFunctionClasspath(basicArgs.udfPath);
        // 拓扑排序保证了opt不会出现 没得到所有输入数据就开始计算的情况
        TopoTraversal topoTraversal = new TopoTraversal(headOperators);
        while (topoTraversal.hasNextOpt()) {
            OperatorBase<Object, Object> curOpt = topoTraversal.nextOpt();
            // 每个operator内部设置一个client，方便和master进行交互
            curOpt.setMasterClient(masterClient);
            curOpt.execute(inputArgs, null);
            // 把计算结果传递到每个下一跳opt
            List<Connection> connections = curOpt.getOutputConnections(); // curOpt没法明确泛化类型
            for (Connection connection : connections) {
                OperatorBase<Object, Object> targetOpt = connection.getTargetOpt();
                topoTraversal.updateInDegree(targetOpt, -1);
                List<Pair<String, String>> keyPairs = connection.getKeys();
                for (Pair<String, String> keyPair : keyPairs) {
                    Object sourceResult = curOpt.getOutputData(keyPair.getValue0());
                    // 将当前opt的输出结果传入下一跳的输入数据
                    targetOpt.setInputData(keyPair.getValue1(), sourceResult);
                }
            }
            logger.info("Stage(" + basicArgs.stageId + ") ———— Current Operator is " + curOpt.getName());
        }
    }

    /**
     * 实际的运行DAG方法
     */
    public void execute() {
        try {
            // 前处理方法
            hook.preHandler(platformArgs);
            // 开始stage
            logger.info("Stage(" + basicArgs.stageId + ")" + " started!");
            masterClient.postStarted();

            // 运行dag
            executeDag();

            // 结束stage
            masterClient.postCompleted();
            logger.info("Stage(" + basicArgs.stageId + ")" + " completed!");
            // 后处理方法
            hook.postHandler(platformArgs);
        } catch (Exception e) {
            // TODO: 错误信息上报给master
            logger.error(e.getMessage());
        }
    }
}
