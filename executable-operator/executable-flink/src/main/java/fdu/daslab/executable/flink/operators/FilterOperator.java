
package fdu.daslab.executable.flink.operators;

import com.beust.jcommander.Parameter;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;

import java.util.List;
import java.util.Map;

/**
 * flink批处理的filter算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/14 15:58
 */
public class FilterOperator extends OperatorBase<DataSet<List<String>>, DataSet<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String filterFunctionName;

    public FilterOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FlinkFilterOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataSet<List<String>>> result) {
        // FilterOperator filterArgs = (FilterOperator) inputArgs.getOperatorParam();
        final String udfName = this.params.get("udfName");
        final DataSet<List<String>> inputData = this.getInputData("data");

        final DataSet<List<String>> nextStream = inputData
                .filter((FilterFunction<List<String>>) data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (boolean) functionModel.invoke(udfName, data);
                });

        this.setOutputData("result", nextStream);

        // 调试代码
//        final DataSet<List<String>> res = this.getOutputData("result");

//        try {
//            res.print();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
    }
}
