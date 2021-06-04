package fdu.daslab.executable.flink.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

import java.util.List;
import java.util.Map;

/**
 * flink批处理的map算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/14 16:45
 */
@Parameters(separators = "=")
public class MapOperator extends OperatorBase<DataSet<List<String>>, DataSet<List<String>>> {

    @Parameter(names = {"--udfName"})
    String mapFunctionName;

    public MapOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FlinkMapOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataSet<List<String>>> result) {
        // MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();

//        final DataSet<List<String>> inputData = this.getInputData("data");
//        @SuppressWarnings("unchecked") final DataSet<List<String>> nextStream = inputData
//                .map(data -> {
//                    // 因为无法序列化，只能传入可序列化的ParamsModel
//                    FunctionModel functionModel = inputArgs.getFunctionModel();
//                    return (List<String>) functionModel.invoke(this.params.get("udfName"), data);
//                })
//                .returns(new TypeHint<List<String>>() {
//                });
//
//        this.setOutputData("result", nextStream);

        final DataSet<List<String>> inputData = this.getInputData("data");

        final String udfName = this.params.get("udfName");

        @SuppressWarnings("unchecked") final DataSet<List<String>> nextStream = inputData
                .map((MapFunction<List<String>, List<String>>) data -> {
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (List<String>) functionModel.invoke(udfName, data);
                })
                .returns(TypeInformation.of(new TypeHint<List<String>>(){}));

        this.setOutputData("result", nextStream);
        final DataSet<List<String>> res = getOutputData("result");

//        try {
//            res.print();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
    }

}
