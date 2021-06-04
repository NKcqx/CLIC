package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.java.DataSet;

import java.util.List;
import java.util.Map;

/**
 * Flink批处理的distinct算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/14 17:32
 */
public class DistinctOperator extends OperatorBase<DataSet<List<String>>, DataSet<List<String>>> {
    public DistinctOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FlinkDistinctOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataSet<List<String>>> result) {
        final DataSet<List<String>> countValue = this.getInputData("data")
                .distinct();
        this.setOutputData("result", countValue);
    }
}
