package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * spark平台的reduceByKey算子，可执行
 *
 * @author 唐志伟
 * @since 2020/7/6 1:53 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class ReduceByKeyOperator implements BasicOperator<JavaRDD<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String reduceFunctionName;

    // 获取key的function
    @Parameter(names = {"--keyName"})
    String keyExtractFunctionName;

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        ReduceByKeyOperator reduceArgs = (ReduceByKeyOperator) inputArgs.getOperatorParam();

        final JavaRDD<List<String>> nextStream = result.getInnerResult()
                .groupBy(data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (String) functionModel.invoke(reduceArgs.keyExtractFunctionName, data);
                })
                .map(groupedData -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    @SuppressWarnings("unchecked")
                    final Optional<List<String>> optionalList = StreamSupport.stream(groupedData._2.spliterator(), true)
                            .reduce((record1, record2) -> (List<String>)
                                    functionModel.invoke(reduceFunctionName, record1, record2));
                    List<String> lineResult = new ArrayList<>();
                    optionalList.ifPresent(lineResult::addAll);
                    return lineResult;
                });
        result.setInnerResult(nextStream);
    }
}
