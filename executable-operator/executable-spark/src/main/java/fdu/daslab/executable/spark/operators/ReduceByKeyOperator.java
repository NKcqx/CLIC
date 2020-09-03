package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class ReduceByKeyOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String reduceFunctionName;

    // 获取key的function
    @Parameter(names = {"--keyName"})
    String keyExtractFunctionName;

    public ReduceByKeyOperator( String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkReduceByOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        // ReduceByKeyOperator reduceArgs = (ReduceByKeyOperator) inputArgs.getOperatorParam();

        final JavaRDD<List<String>> nextStream = this.getInputData("data")
                .groupBy(data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (String) functionModel.invoke(this.params.get("keyName"), data);
                })
                .map(groupedData -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    @SuppressWarnings("unchecked")
                    final Optional<List<String>> optionalList = StreamSupport.stream(groupedData._2.spliterator(), true)
                            .reduce((record1, record2) -> (List<String>)
                                    functionModel.invoke(this.params.get("udfName"), record1, record2));
                    List<String> lineResult = new ArrayList<>();
                    optionalList.ifPresent(lineResult::addAll);
                    return lineResult;
                });
        this.setOutputData("result", nextStream);
    }
}
