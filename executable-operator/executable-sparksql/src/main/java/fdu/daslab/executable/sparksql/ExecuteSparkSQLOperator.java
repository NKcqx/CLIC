package fdu.daslab.executable.sparksql;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.TopTraversal;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import fdu.daslab.executable.sparksql.operators.SparkSQLOperatorFactory;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Array;

import java.io.File;

import java.util.*;

/**
 * SparkSQL平台的operator的具体实现，兼顾算子融合
 * 按照--dagPath指定实际的算子DAG图
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/8 4:30 PM
 */
@Parameters(separators = "=")
public class ExecuteSparkSQLOperator {
    @Parameter(names = {"--dagPath", "-dag"})
    String dagPath;

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ExecuteSparkSQLOperator.class);
        ExecuteSparkSQLOperator entry = new ExecuteSparkSQLOperator();
        JCommander.newBuilder()
                .addObject(entry)
                .build()
                .parse(args);
        long start = System.currentTimeMillis();
        logger.info("Stage(sparkSql) ———— Start A New SparkSQL Stage");
        try {
            OperatorBase headOperator = ArgsUtil.parseArgs(entry.dagPath, new SparkSQLOperatorFactory());
            if (headOperator.getParams().containsKey("inputPath")) {
                String inputFile = (String) headOperator.getParams().get("inputPath");
                File f = new File(inputFile);
                if (f.exists() && f.isFile()) {
                    logger.info("Stage(sparkSql) ———— Input file size:  " + f.length());
                } else {
                    logger.info("Stage(sparkSql) ———— File doesn't exist or it is not a file");
                }
            }
            // 遍历DAG，执行execute，每次执行前把上一跳的输出结果放到下一跳的输入槽中（用Connection来转移ResultModel里的数据）
            ParamsModel inputArgs = new ParamsModel(null);
            // 拓扑排序保证了opt不会出现 没得到所有输入数据就开始计算的情况
            TopTraversal topTraversal = new TopTraversal(headOperator);
            OperatorBase tailOperator = null;
            while (topTraversal.hasNextOpt()) {
                OperatorBase<RDD<Array<String>>, RDD<Array<String>>> curOpt = topTraversal.nextOpt();
                curOpt.execute(inputArgs, null);
                // 把计算结果传递到每个下一跳opt
                List<Connection> connections = curOpt.getOutputConnections();
                for (Connection connection : connections) {
                    OperatorBase<RDD<Array<String>>, RDD<Array<String>>> targetOpt = connection.getTargetOpt();
                    String sourceKey = connection.getSourceKey();
                    String targetKey = connection.getTargetKey();
                    RDD<Array<String>> sourceResult = curOpt.getOutputData(sourceKey);
                    targetOpt.setInputData(targetKey, sourceResult);
                }
                logger.info("Stage(sparkSql) ———— Current SparkSQL Operator is " + curOpt.getName());
                tailOperator = curOpt;
            }
            long end = System.currentTimeMillis(); //获取结束时间
            logger.info("Stage(sparkSql) ———— Running hold time:  " + (end - start) + "ms");

            if (tailOperator != null && tailOperator.getParams().containsKey("outputPath")) {
                String outputPath = (String) tailOperator.getParams().get("outputPath");
                File f = new File(outputPath);
                if (f.exists() && f.isFile()) {
                    logger.info("Stage(sparkSql) ———— Output file size:  " + f.length());
                } else {
                    logger.info("Stage(sparkSql) ———— File doesn't exist or it is not a file");
                }
            }
            logger.info("Stage(sparkSql) ———— End The Current Spark Stage");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
