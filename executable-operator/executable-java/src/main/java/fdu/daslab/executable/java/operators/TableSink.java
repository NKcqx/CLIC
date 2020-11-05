package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import joinery.DataFrame;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 将Table写入文件的算子
 *
 * @author 刘丰艺
 * @since 2020/11/5 8:15 PM
 * @version 1.0
 */
public class TableSink extends OperatorBase<DataFrame<Object>, DataFrame<Object>> {

    public TableSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaTableSinkOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataFrame<Object>> result) {
        try {
            // 如果用户提供的文件路径后缀为.txt，那么存储为txt文件
            // 如果后缀为非.csv非.txt或者没有任何后缀，那么以txt格式存储
            this.getInputData("data").writeCsv(this.params.get("outputPath"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
