package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import joinery.DataFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 将DataFrame<Object>转换成Stream<List<String>>
 * joiner库实现了java的DataFrame，本质是List<List<T>>矩阵
 * 这种DataFrame既有列属性名也有行属性名
 * 可以以矩阵的形式(row,col)访问某一行某一列的数据
 *
 * @author 刘丰艺
 * @since 2020/11/3 3:30 PM
 * @version 1.0
 */
public class FromTableOperator extends OperatorBase<DataFrame<Object>, Stream<List<String>>> {

    public FromTableOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaFromTableOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        DataFrame<Object> df = this.getInputData("data");
        List<List<String>> convertResult = new ArrayList<>();
        for (Object row : df.index()) {
            List<String> line = new ArrayList<>();
            for (Object col : df.columns()) {
                line.add((String) df.get(row, col));
            }
            convertResult.add(line);
        }
        this.setOutputData("result", convertResult.stream());
    }
}
