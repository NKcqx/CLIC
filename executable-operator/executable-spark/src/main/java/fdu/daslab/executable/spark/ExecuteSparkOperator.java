package fdu.daslab.executable.spark;

import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.spark.constants.SparkOperatorEnums;
import fdu.daslab.executable.spark.model.RddResult;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * spark平台的operator的具体实现，兼顾算子融合，因为是线性，只支持线性的合并，不支持 n - 1 ===> 依赖前面的pipeline同步，不依赖具体执行
 *
 * 按照--udfPath指定用户定义的类的位置
 * 按照--master --appName --... 以及一些其他参数设置spark的配置信息
 * 按照--operator指定实际的算子，可以指定多个，多个在一个平台上，后台统一执行
 */
public class ExecuteSparkOperator {

    public static void main(String[] args) {
        // 用户定义的函数放在一个文件里面
        String functionPath = args[0].substring(args[0].indexOf("=") + 1);
        // 可能存在一些spark配置数据，暂时没有

        // 这个Map是有序的
        Map<String, String[]> standaloneArgs = ArgsUtil.separateArgsByKey(
                Arrays.copyOfRange(args, 1, args.length), "--operator");
        // 目前支持的算子
        Map<String, BasicOperator<JavaRDD<List<String>>>> allOperators = SparkOperatorEnums.getAllOperators();
        // spark平台的每个算子（除了sink），均返回一个JavaRDD
        RddResult result = new RddResult();

        standaloneArgs.forEach((operator, operatorArgs) -> {
            // 目前执行的算子
            BasicOperator<JavaRDD<List<String>>> curOperator = allOperators.get(operator);
            // 解析参数
            ArgsUtil.parseArgs(curOperator, operatorArgs);
            ParamsModel<JavaRDD<List<String>>> inputArgs = new ParamsModel<>(curOperator, null);
            // FunctionModel为空，因为无法序列化，手动设置路径
            inputArgs.setFunctionClasspath(functionPath);
            // 实际的运行
            curOperator.execute(inputArgs, result);
        });
    }
}
