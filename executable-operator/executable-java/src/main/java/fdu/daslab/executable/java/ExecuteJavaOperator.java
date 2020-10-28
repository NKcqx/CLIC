
package fdu.daslab.executable.java;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import fdu.daslab.executable.basic.utils.TopoTraversal;
import fdu.daslab.executable.java.constants.JavaOperatorFactory;
import fdu.daslab.service.client.SchedulerServiceClient;
import org.apache.thrift.TException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;

/**
 * Java平台的operator的具体实现，兼顾算子融合
 * 因为是线性，只支持线性的合并，不支持 n - 1 ===>
 * 依赖前面的pipeline同步，不依赖具体执行
 * <p>
 * 按照--udfPath指定用户定义的类的位置
 * 按照--operator指定实际的算子，可以指定多个，多个在一个平台上，后台统一执行
 *
 * @author 唐志伟，陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:48 PM
 */
@Parameters(separators = "=")
public class ExecuteJavaOperator {
    @Parameter(names = {"--stageId", "-sid"})
    String stageId;    // stage的全局唯一标识

    @Parameter(names = {"--udfPath", "-udf"})
    String udfPath;

    @Parameter(names = {"--dagPath", "-dag"})
    String dagPath;

//    @Parameter(names = {"--port", "-p"})
//    Integer thriftPort; // 本server启动的thrift端口

    @Parameter(names = {"--masterHost", "-mh"})
    String masterHost; // master的thrift地址

    @Parameter(names = {"--masterPort", "-mp"})
    Integer masterPort; // master的thrift端口

    public static void main(String[] args) throws TException {
        Logger logger = LoggerFactory.getLogger(ExecuteJavaOperator.class);
        // 解析命令行参数
        ExecuteJavaOperator entry = new ExecuteJavaOperator();
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
        logger.info("Stage(java) ———— Start A New Java Stage");
        // 解析YAML文件，构造DAG
        try {
            InputStream yamlStream = new FileInputStream(new File(entry.dagPath));
            Pair<List<OperatorBase>, List<OperatorBase>> headAndEndsOperators =
                    ArgsUtil.parseArgs(yamlStream, new JavaOperatorFactory());
            // 遍历DAG，执行execute，每次执行前把上一跳的输出结果放到下一跳的输入槽中（用Connection来转移ResultModel里的数据）
            ParamsModel inputArgs = new ParamsModel(functionModel);
            // 拓扑排序保证了opt不会出现 没得到所有输入数据就开始计算的情况
            TopoTraversal topoTraversal = new TopoTraversal(headAndEndsOperators.getValue0());
            while (topoTraversal.hasNextOpt()) {
                OperatorBase<Stream<List<String>>, Stream<List<String>>> curOpt = topoTraversal.nextOpt();
                // 每个operator内部设置一个client，方便和master进行交互
                curOpt.setMasterClient(masterClient);
                curOpt.execute(inputArgs, null);
                // 把计算结果传递到每个下一跳opt
                List<Connection> connections = curOpt.getOutputConnections(); // curOpt没法明确泛化类型
                for (Connection connection : connections) {
                    OperatorBase<Stream<List<String>>, Stream<List<String>>> targetOpt = connection.getTargetOpt();
                    topoTraversal.updateInDegree(targetOpt, -1);

                    List<Pair<String, String>> keyPairs = connection.getKeys();
                    for (Pair<String, String> keyPair : keyPairs) {
                        Stream<List<String>> sourceResult = curOpt.getOutputData(keyPair.getValue0());
                        // 将当前opt的输出结果传入下一跳的输入数据
                        targetOpt.setInputData(keyPair.getValue1(), sourceResult);
                    }
                }
                logger.info("Stage(java) ———— Current Java Operator is " + curOpt.getName());
            }
            long end = System.currentTimeMillis(); //获取结束时间
            logger.info("Stage(java) ———— Running hold time:： " + (end - start) + "ms");
            logger.info("Stage(java) ———— End The Current Java Stage");

        } catch (Exception e) {
            e.printStackTrace();
        }

        // 结束stage
        masterClient.postCompleted();
    }
}
