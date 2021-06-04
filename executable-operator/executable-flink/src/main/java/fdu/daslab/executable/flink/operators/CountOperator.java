package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.java.DataSet;

import java.util.List;
import java.util.Map;

/**
 *
 * flink批处理的count算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/11 16:32
 */
public class CountOperator extends OperatorBase<DataSet<List<String>>, Long> {
    public CountOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FlinkCountOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Long> result) {
        try {
            final long countValue = this.getInputData("data")
                    .count();
            this.setOutputData("result", countValue);
        } catch (Exception e){
            // e.printStackTrace();
        }
    }
}
