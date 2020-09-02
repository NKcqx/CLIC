package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BiOptParamsModel;
import fdu.daslab.executable.basic.model.BinaryBasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * spark平台的join算子
 *
 * @author 刘丰艺
 * @since 2020/8/22 4:50 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class JoinOperator implements BinaryBasicOperator<JavaRDD<List<String>>> {

    @Parameter(names = {"--leftTableKeyName"})
    String leftTableKeyName;

    @Parameter(names = {"--rightTableKeyName"})
    String rightTableKeyName;

    @Parameter(names = {"--leftTableFuncName"})
    String leftTableFuncName;

    @Parameter(names = {"--rightTableFuncName"})
    String rightTableFuncName;

    @Override
    public void execute(BiOptParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> input1,
                        ResultModel<JavaRDD<List<String>>> input2,
                        ResultModel<JavaRDD<List<String>>> result) {
        JoinOperator joinArgs = (JoinOperator) inputArgs.getOperatorParam();

        // First JavaRDD
        JavaPairRDD<String, List<String>> firstRDD = input1.getInnerResult()
                .mapToPair(new PairFunction<List<String>, String, List<String>>() {
            @Override
            public Tuple2<String, List<String>> call(List<String> line) throws Exception {
                FunctionModel joinFunction = inputArgs.getFunctionModel();
                // 用户指定key
                // 用户指定join时左表要select哪几列
                return new Tuple2<>((String) joinFunction.invoke(joinArgs.leftTableKeyName, line),
                        (List<String>) joinFunction.invoke(joinArgs.leftTableFuncName, line));
            }
        });

        // Second JavaRDD
        JavaPairRDD<String, List<String>> secondRDD = input2.getInnerResult()
                .mapToPair(new PairFunction<List<String>, String, List<String>>() {
            @Override
            public Tuple2<String, List<String>> call(List<String> line) throws Exception {
                FunctionModel joinFunction = inputArgs.getFunctionModel();
                // 用户指定key
                // 用户指定join时左表要select哪几列
                return new Tuple2<>((String) joinFunction.invoke(joinArgs.rightTableKeyName, line),
                        (List<String>) joinFunction.invoke(joinArgs.rightTableFuncName, line));
            }
        });

        // Join
        JavaPairRDD<String, Tuple2<List<String>, List<String>>> joinRDD = firstRDD.join(secondRDD);

        JavaRDD<List<String>> nextStream = joinRDD.map(
                new Function<Tuple2<String, Tuple2<List<String>, List<String>>>, List<String>>() {
            @Override
            public List<String> call(Tuple2<String, Tuple2<List<String>, List<String>>> stringTuple2Tuple2)
                    throws Exception {
                List<String> resultLine = new ArrayList<>();
                // key
                resultLine.add(stringTuple2Tuple2._1());
                // first table
                resultLine.addAll(stringTuple2Tuple2._2()._1());
                // second table
                resultLine.addAll(stringTuple2Tuple2._2()._2());
                return Collections.singletonList(String.join(",", resultLine));
            }
        });

        result.setInnerResult(nextStream);
    }
}
