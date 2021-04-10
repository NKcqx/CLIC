package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.List;
import java.util.Map;

/**
 * flink批处理，将数据keyBy后Reduce
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/14 16:48
 */
public class ReduceByKeyOperator extends OperatorBase<DataSet<List<String>>, DataSet<List<String>>> {

    public ReduceByKeyOperator(String id,
                               List<String> inputKeys,
                               List<String> outputKeys,
                               Map<String, String> params) {
        super("FlinkReduceByOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataSet<List<String>>> result) {
        // ReduceByKeyOperator reduceArgs = (ReduceByKeyOperator) inputArgs.getOperatorParam();
        final String keyName = this.params.get("keyName");
        final String udfName = this.params.get("udfName");
        final DataSet<List<String>> nextStream = this.getInputData("data")
                .groupBy((KeySelector<List<String>, String>) data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (String) functionModel.invoke(keyName, data);
                })
                .reduce((ReduceFunction<List<String>>)(record1, record2) -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (List<String>)
                            functionModel.invoke(udfName, record1, record2);
                });
        this.setOutputData("result", nextStream);

        final DataSet<List<String>> res = this.getOutputData("result");

//        try {
//            res.print();
//        } catch (Exception e){
//            e.printStackTrace();
//        }

    }
}
