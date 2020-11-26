package fdu.daslab.executable.graphchi;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import fdu.daslab.executable.basic.utils.TopoTraversal;
import fdu.daslab.executable.graphchi.constants.GraphchiOperatorFactory;
import fdu.daslab.service.client.SchedulerServiceClient;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/23 16:06
 */
@Parameters(separators = "=")
public class ExecutableGraphchiOperator {
    @Parameter(names = {"--stageId", "-sid"})
    String stageId = null;    // stage的全局唯一标识

    @Parameter(names = {"--udfPath", "-udf"})
    String udfPath;

    @Parameter(names = {"--dagPath", "-dag"})
    String dagPath;

    @Parameter(names = {"--masterHost", "-mh"})
    String masterHost = null; // master的thrift地址

    @Parameter(names = {"--masterPort", "-mp"})
    Integer masterPort = null; // master的thrift端口

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(ExecutableGraphchiOperator.class);
        // 解析命令行参数
        ExecutableGraphchiOperator entry = new ExecutableGraphchiOperator();
        JCommander.newBuilder()
                .addObject(entry)
                .build()
                .parse(args);

        // 创建一个thrift client，用于和master进行交互
        SchedulerServiceClient masterClient = new SchedulerServiceClient(entry.stageId,
                entry.masterHost, entry.masterPort);

        // 开始stage
        masterClient.postStarted();

        final FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath(entry.udfPath);

        //记录时间
        long start = System.currentTimeMillis();   //获取开始时间
        logger.info("Stage(Graphchi) ———— Start A New Graphchi Stage");
        // 解析YAML文件，构造DAG

        InputStream yamlStream = new FileInputStream(new File(entry.dagPath));
        Pair<List<OperatorBase>, List<OperatorBase>> headAndEndsOperators =
                ArgsUtil.parseArgs(yamlStream, new GraphchiOperatorFactory());
        // 遍历DAG，执行execute，每次执行前把上一跳的输出结果放到下一跳的输入槽中（用Connection来转移ResultModel里的数据）
        ParamsModel inputArgs = new ParamsModel(functionModel);
        // 拓扑排序保证了opt不会出现 没得到所有输入数据就开始计算的情况
        TopoTraversal topoTraversal = new TopoTraversal(headAndEndsOperators.getValue0());
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
            logger.info("Stage(Graphchi) ———— Current Graphchi Operator is " + curOpt.getName());
        }
        long end = System.currentTimeMillis(); //获取结束时间
        logger.info("Stage(Graphchi) ———— Running hold time:： " + (end - start) + "ms");
        logger.info("Stage(Graphchi) ———— End The Current Graphchi Stage");

        // 结束stage
        masterClient.postCompleted();
    }

}
