package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * spark平台的join算子
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/8/22 4:50 PM
 */
@Parameters(separators = "=")
public class JoinOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

    public JoinOperator(String id,
                        List<String> inputKeys,
                        List<String> outputKeys,
                        Map<String, String> params) {
        super("JoinOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        String leftKey = this.params.get("leftKey");
        String rightKey = this.params.get("rightKey");
        String leftCols = this.params.get("leftCols");
        String rightCols = this.params.get("rightCols");

        if (leftKey == null) {
            throw new NoSuchElementException("key of left table must not be empty!");
        }
        if (rightKey == null) {
            throw new NoSuchElementException("key of right table must not be empty!");
        }

        // First JavaRDD
        JavaPairRDD<String, List<String>> firstRDD = this.getInputData("leftTable")
                .mapToPair((PairFunction<List<String>, String, List<String>>) line -> {
                    FunctionModel joinFunction = inputArgs.getFunctionModel();
                    // 用户指定key
                    String tableKey = (String) joinFunction.invoke(leftKey, line);
                    List<String> tableLine = null;
                    if (leftCols != null) {
                        // 用户指定join时左表要select哪几列
                        tableLine = (List<String>) joinFunction.invoke(leftCols, line);
                    } else {
                        tableLine = line;
                    }
                    return new Tuple2<>(tableKey, tableLine);
                });

        // Second JavaRDD
        JavaPairRDD<String, List<String>> secondRDD = this.getInputData("rightTable")
                .mapToPair((PairFunction<List<String>, String, List<String>>) line -> {
                    FunctionModel joinFunction = inputArgs.getFunctionModel();
                    // 用户指定key
                    String tableKey = (String) joinFunction.invoke(rightKey, line);
                    List<String> tableLine = null;
                    if (rightCols != null) {
                        // 用户指定join时右表要select哪几列
                        tableLine = (List<String>) joinFunction.invoke(rightCols, line);
                    } else {
                        tableLine = line;
                    }
                    return new Tuple2<>(tableKey, tableLine);
                });

        // Join
        JavaPairRDD<String, Tuple2<List<String>, List<String>>> joinRDD = firstRDD.join(secondRDD);
        JavaRDD<List<String>> nextStream = joinRDD.map(
                (Function<Tuple2<String, Tuple2<List<String>, List<String>>>, List<String>>) stringTuple2Tuple2 -> {
                    List<String> resultLine = new ArrayList<>();
                    if (leftCols != null) {
                        // key
                        resultLine.add(stringTuple2Tuple2._1);
                    }
                    // first table
                    resultLine.addAll(stringTuple2Tuple2._2()._1());
                    // second table
                    resultLine.addAll(stringTuple2Tuple2._2()._2());
                    return Collections.singletonList(String.join(",", resultLine));
                });

        this.setOutputData("result", nextStream);
    }
}
