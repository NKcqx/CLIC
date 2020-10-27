package fdu.daslab.executable.sparksql;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.TopoTraversal;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.sparksql.constants.SparkSQLOperatorFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
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
            InputStream yamlStream = new FileInputStream(new File(entry.dagPath));
            Pair<List<OperatorBase>, List<OperatorBase>> headAndEndOperators =
                    ArgsUtil.parseArgs(yamlStream, new SparkSQLOperatorFactory());
            // 遍历DAG，执行execute，每次执行前把上一跳的输出结果放到下一跳的输入槽中（用Connection来转移ResultModel里的数据）
            ParamsModel inputArgs = new ParamsModel(null);
            // 拓扑排序保证了opt不会出现 没得到所有输入数据就开始计算的情况
            TopoTraversal topTraversal = new TopoTraversal(headAndEndOperators.getValue0());
            while (topTraversal.hasNextOpt()) {
                OperatorBase<Dataset<Row>, Dataset<Row>> curOpt = topTraversal.nextOpt();
                curOpt.execute(inputArgs, null);
                // 把计算结果传递到每个下一跳opt
                List<Connection> connections = curOpt.getOutputConnections();
                for (Connection connection : connections) {
                    OperatorBase<Dataset<Row>, Dataset<Row>> targetOpt = connection.getTargetOpt();
                    topTraversal.updateInDegree(targetOpt, -1);
                    List<Pair<String, String>> keyPairs = connection.getKeys();
                    for (Pair<String, String> keyPair : keyPairs) {
                        Dataset<Row> sourceResult = curOpt.getOutputData(keyPair.getValue0());
                        targetOpt.setInputData(keyPair.getValue1(), sourceResult);
                    }
                }
                logger.info("Stage(sparkSql) ———— Current SparkSQL Operator is " + curOpt.getName());
            }

            long end = System.currentTimeMillis(); //获取结束时间
            logger.info("Stage(sparkSql) ———— Running hold time:  " + (end - start) + "ms");
            logger.info("Stage(sparkSql) ———— End The Current Spark Stage");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
