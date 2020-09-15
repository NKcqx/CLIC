package fdu.daslab.executable.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.TopTraversal;
import fdu.daslab.executable.spark.operators.SparkOperatorFactory;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * spark平台的operator的具体实现，兼顾算子融合，因为是线性，只支持线性的合并，不支持 n - 1
 * ===> 依赖前面的pipeline同步，不依赖具体执行
 * <p>
 * 按照--udfPath指定用户定义的类的位置
 * 按照--master --appName --... 以及一些其他参数设置spark的配置信息
 * 按照--operator指定实际的算子，可以指定多个，多个在一个平台上，后台统一执行
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:55 PM
 */
@Parameters(separators = "=")
public class ExecuteSparkOperator {

    @Parameter(names = {"--udfPath", "-udf"})
    String udfPath;
    @Parameter(names = {"--dagPath", "-dag"})
    String dagPath;

    public static void main(String[] args) {
        ExecuteSparkOperator entry = new ExecuteSparkOperator();
        JCommander.newBuilder()
                .addObject(entry)
                .build()
                .parse(args);
        Logger logger = LoggerFactory.getLogger(ExecuteSparkOperator.class);
        // String functionPath =  entry.udfPath;
        // final FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath(entry.udfPath);
        //记录时间
        long start = System.currentTimeMillis();   //获取开始时间
        try {
            OperatorBase headOperator = ArgsUtil.parseArgs(entry.dagPath, new SparkOperatorFactory());
            //记录输入文件的大小
            if (headOperator.getParams().containsKey("inputPath")) {
                String inputFile = (String) headOperator.getParams().get("inputPath");
                File f = new File(inputFile);
                if (f.exists() && f.isFile()) {
                    logger.info("input file size :" + f.length());
                } else {
                    logger.info("file doesn't exist or is not a file");
                }
            }
            // 遍历DAG，执行execute，每次执行前把上一跳的输出结果放到下一跳的输入槽中（用Connection来转移ResultModel里的数据）
            ParamsModel inputArgs = new ParamsModel(null);
            inputArgs.setFunctionClasspath(entry.udfPath);
            // 拓扑排序保证了opt不会出现 没得到所有输入数据就开始计算的情况
            TopTraversal topTraversal = new TopTraversal(headOperator);
            OperatorBase tailOperator = null;
            while (topTraversal.hasNextOpt()) {
                OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> curOpt = topTraversal.nextOpt();
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
                tailOperator = curOpt;
            }
            long end = System.currentTimeMillis(); //获取结束时间
            logger.info("程序运行时间： " + (end - start) + "ms");

            if (tailOperator != null && tailOperator.getParams().containsKey("outputPath")) {
                String outputPath = (String) tailOperator.getParams().get("outputPath");
                File f = new File(outputPath);
                if (f.exists() && f.isFile()) {
                    logger.info("output file size :" + f.length());
                } else {
                    logger.info("file doesn't exist or is not a file");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
