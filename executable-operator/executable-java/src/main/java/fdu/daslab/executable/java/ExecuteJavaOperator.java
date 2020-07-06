package fdu.daslab.executable.java;

import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.java.constants.JavaOperatorEnums;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.java.model.StreamResult;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.ReflectUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Java平台的operator的具体实现，兼顾算子融合，因为是线性，只支持线性的合并，不支持 n - 1 ===>
 * 依赖前面的pipeline同步，不依赖具体执行
 *
 * 按照--udfPath指定用户定义的类的位置
 * 按照--operator指定实际的算子，可以指定多个，多个在一个平台上，后台统一执行
 *
 * @author 唐志伟
 * @since 2020/7/6 1:48 PM
 * @version 1.0
 */
public class ExecuteJavaOperator {

    public static void main(String[] args) {
        // 用户定义的函数放在一个文件里面
        final FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath(
                args[0].substring(args[0].indexOf("=") + 1));
        // 这个Map是有序的
        Map<String, String[]> standaloneArgs = ArgsUtil.separateArgsByKey(
                Arrays.copyOfRange(args, 1, args.length), "--operator");
        // 目前支持的算子
        Map<String, BasicOperator<Stream<List<String>>>> allOperators = JavaOperatorEnums.getAllOperators();
        // java平台的每个算子（除了sink），均返回一个Stream
        StreamResult result = new StreamResult();
        standaloneArgs.forEach((operator, operatorArgs) -> {
            // 目前执行的算子
            BasicOperator<Stream<List<String>>> curOperator = allOperators.get(operator);
            // 解析参数
            ArgsUtil.parseArgs(curOperator, operatorArgs);
            ParamsModel<Stream<List<String>>> inputArgs = new ParamsModel<>(curOperator, functionModel);
            // 实际的运行
            curOperator.execute(inputArgs, result);
        });
    }
}
