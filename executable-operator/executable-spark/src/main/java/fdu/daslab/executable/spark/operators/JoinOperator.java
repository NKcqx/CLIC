package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * spark平台的join算子
 *
 * @author 刘丰艺
 * @since 2020/8/22 4:50 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class JoinOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

    public JoinOperator(String id,
                        List<String> inputKeys,
                        List<String> outputKeys,
                        Map<String, String> params) {
        super("SparkJoinOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {

        String leftTableKey = this.params.get("leftTableKeyName");
        String leftTableFunc = this.params.get("leftTableFuncName");

        String rightTableKey = this.params.get("rightTableKeyName");
        String rightTableFunc = this.params.get("rightTableFuncName");

        // First JavaRDD
        JavaPairRDD<String, List<String>> firstRDD = this.getInputData("leftTable")
                .mapToPair((PairFunction<List<String>, String, List<String>>) line -> {
                    FunctionModel joinFunction = inputArgs.getFunctionModel();
                    // 用户指定key
                    String tableKey = (String) joinFunction.invoke(leftTableKey, line);
                    // 用户指定join时左表要select哪几列
                    List<String> tableLine = (List<String>) joinFunction.invoke(leftTableFunc, line);
                    return new Tuple2<>(tableKey, tableLine);
                });

        // Second JavaRDD
        JavaPairRDD<String, List<String>> secondRDD = this.getInputData("rightTable")
                .mapToPair((PairFunction<List<String>, String, List<String>>) line -> {
                    FunctionModel joinFunction = inputArgs.getFunctionModel();
                    // 用户指定key
                    String tableKey = (String) joinFunction.invoke(rightTableKey, line);
                    // 用户指定join时右表要select哪几列
                    List<String> tableLine = (List<String>) joinFunction.invoke(rightTableFunc, line);
                    return new Tuple2<>(tableKey, tableLine);
                });

        // Join
        JavaPairRDD<String, Tuple2<List<String>, List<String>>> joinRDD = firstRDD.join(secondRDD);

        JavaRDD<List<String>> nextStream = joinRDD.map(
                (Function<Tuple2<String, Tuple2<List<String>, List<String>>>, List<String>>) stringTuple2Tuple2 -> {
                    List<String> resultLine = new ArrayList<>();
                    // key
                    resultLine.add(stringTuple2Tuple2._1());
                    // first table
                    resultLine.addAll(stringTuple2Tuple2._2()._1());
                    // second table
                    resultLine.addAll(stringTuple2Tuple2._2()._2());
                    return Collections.singletonList(String.join(",", resultLine));
                });
        this.setOutputData("result", nextStream);
    }
}
