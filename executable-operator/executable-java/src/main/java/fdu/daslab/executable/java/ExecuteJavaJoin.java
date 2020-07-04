package fdu.daslab.executable.java;

import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import fdu.daslab.executable.java.constants.JavaOperatorEnums;
import fdu.daslab.executable.java.model.StreamResult;
import fdu.daslab.executable.java.operators.FileSink;
import fdu.daslab.executable.java.operators.FileSource;
import fdu.daslab.executable.java.operators.FilterOperator;
import fdu.daslab.executable.java.operators.JoinOperator;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 测试 Java Join Operator 是否可用的main函数类
 */
public class ExecuteJavaJoin {

    public static void main(String[] args) {

        /**
         * 命令行参数
         */
//        --udfPath=D:/IRDemo/executable-operator/output-class/fdu/daslab/executable/udf/TestJoinCaseFunc.class
//                --input=D:/IRDemo/executable-operator/executable-basic/src/main/resources/data/join/webCompany.csv
//                --udfName=filterWebCompanyFunc
//                --input=D:/IRDemo/executable-operator/executable-basic/src/main/resources/data/join/companyInfo.csv
//                --udfName=filterCompanyInfoFunc
//                --leftTableKeyName=leftTableKey
//                --rightTableKeyName=rightTableKey
//                --leftTableFuncName=leftTableFunc
//                --rightTableFuncName=rightTableFunc
//                --output=D:/IRDemo/executable-operator/executable-basic/src/main/resources/data/join/joinResult.csv

        final FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath(
                args[0].substring(args[0].indexOf("=") + 1)
        );
        StreamResult tableOneResult = new StreamResult();
        StreamResult tableTwoResult = new StreamResult();
        StreamResult joinResult = new StreamResult();

        System.out.println("----------------first stream----------------");
        BasicOperator<Stream<List<String>>> wayOne1 = new FileSource();
        String[] wayOne1Args = {args[1]};
        ArgsUtil.parseArgs(wayOne1, wayOne1Args);
        ParamsModel<Stream<List<String>>> wayOne1InputArgs = new ParamsModel<>(wayOne1, functionModel);
        wayOne1.execute(wayOne1InputArgs, tableOneResult);

        BasicOperator<Stream<List<String>>> wayOne2 = new FilterOperator();
        String[] wayOne2Args = {args[2]};
        ArgsUtil.parseArgs(wayOne2, wayOne2Args);
        ParamsModel<Stream<List<String>>> wayOne2InputArgs = new ParamsModel<>(wayOne2, functionModel);
        wayOne2.execute(wayOne2InputArgs, tableOneResult);

        System.out.println("----------------second stream----------------");
        BasicOperator<Stream<List<String>>> wayTwo1 = new FileSource();
        String[] wayTwo1Args = {args[3]};
        ArgsUtil.parseArgs(wayTwo1, wayTwo1Args);
        ParamsModel<Stream<List<String>>> wayTwo1InputArgs = new ParamsModel<>(wayTwo1, functionModel);
        wayTwo1.execute(wayTwo1InputArgs, tableTwoResult);

        BasicOperator<Stream<List<String>>> wayTwo2 = new FilterOperator();
        String[] wayTwo2Args = {args[4]};
        ArgsUtil.parseArgs(wayTwo2, wayTwo2Args);
        ParamsModel<Stream<List<String>>> wayTwo2InputArgs = new ParamsModel<>(wayTwo2, functionModel);
        wayTwo2.execute(wayTwo2InputArgs, tableTwoResult);

        System.out.println("----------------two streams join----------------");
        BinaryBasicOperator<Stream<List<String>>> joinOpt = new JoinOperator();
        String[] joinArgs = {args[5], args[6], args[7], args[8]};
        ArgsUtil.parseArgs(joinOpt, joinArgs);
        BiOptParamsModel<Stream<List<String>>> joinInputArgs = new BiOptParamsModel<>(joinOpt, functionModel);
        joinOpt.execute(joinInputArgs, tableOneResult, tableTwoResult, joinResult);
        System.out.println("----------------join finish----------------");

        BasicOperator<Stream<List<String>>> joinSink = new FileSink();
        String[] joinSinkArgs = {args[9]};
        ArgsUtil.parseArgs(joinSink, joinSinkArgs);
        ParamsModel<Stream<List<String>>> joinSinkInputArgs = new ParamsModel<>(joinSink, functionModel);
        joinSink.execute(joinSinkInputArgs, joinResult);
        System.out.println("----------------join result sink finish----------------");
    }
}
