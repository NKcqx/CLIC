package fdu.daslab.executable.java.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import joinery.DataFrame;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 将Stream<List<String>>转换成DataFrame<Object>
 * joiner库实现了java的DataFrame，本质是List<List<T>>矩阵
 * 这种DataFrame既有列属性名也有行属性名
 * 可以以矩阵的形式(row,col)访问某一行某一列的数据
 *
 * @author 刘丰艺
 * @since 2020/11/3 3:30 PM
 * @version 1.0
 */
public class ToTableOperator extends OperatorBase<Stream<List<String>>, DataFrame<Object>> {

    public ToTableOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaToTableOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataFrame<Object>> result) {
        FunctionModel functionModel = inputArgs.getFunctionModel();

        // 获取用户定义的schemaMap
        Map<String, Triplet<Class, Boolean, String>> schemaMap =
                (Map<String, Triplet<Class, Boolean, String>>) functionModel.invoke(this.params.get("udfName"));
        List<String> tableSchema = getSchemaFromMap(schemaMap);
        DataFrame<Object> convertResult = new DataFrame<>(tableSchema);
        this.getInputData("data").forEach(data -> {
            // 不指定行属性名，则其为从0开始递增的正整数
            convertResult.append(data);
        });
        this.setOutputData("result", convertResult);
    }

    /**
     * 将用户定义的schemaMap转成schema
     *
     * @param schemaMap
     * @return
     */
    private List<String> getSchemaFromMap(Map<String, Triplet<Class, Boolean, String>> schemaMap) {
        int i = 0;
        List<String> fieldNames = new ArrayList<>();
        for (String key : schemaMap.keySet()) {
            fieldNames.add(key);
        }
        return fieldNames;
    }
}
