package fdu.daslab.executable.spark;

import fdu.daslab.executable.basic.model.BiOptParamsModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ArgsUtil;
import fdu.daslab.executable.spark.model.RddResult;
import fdu.daslab.executable.spark.operators.FileSink;
import fdu.daslab.executable.spark.operators.FileSource;
import fdu.daslab.executable.spark.operators.FilterOperator;
import fdu.daslab.executable.spark.operators.JoinOperator;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * 这个可执行类是用来测试spark平台的join算子是否可用
 *
 * @author 刘丰艺
 * @since 2020/8/22 5:35 PM
 * @version 1.0
 */
public class ExecuteSparkJoin {

    /**
     * 命令行参数
     */
//        --udfPath=D:/IRDemo/executable-operator/executable-basic/target/classes
//        /fdu/daslab/executable/udf/TestJoinCaseFunc.class
//                --input=D:/IRDemo/executable-operator/executable-basic/src/main/resources/data/join/webCompany.csv
//                --udfName=filterWebCompanyFunc
//                --input=D:/IRDemo/executable-operator/executable-basic/src/main/resources/data/join/companyInfo.csv
//                --udfName=filterCompanyInfoFunc
//                --leftTableKeyName=leftTableKey
//                --rightTableKeyName=rightTableKey
//                --leftTableFuncName=leftTableFunc
//                --rightTableFuncName=rightTableFunc
//                --output=D:/IRDemo/executable-operator/executable-basic
//                /src/main/resources/data/join/sparkJoinResult.csv
    public static void main(String[] args) {
        // 用户定义的函数放在一个文件里面
//        String functionPath = args[0].substring(args[0].indexOf("=") + 1);
//
//        RddResult tableOneResult = new RddResult();
//        RddResult tableTwoResult = new RddResult();
//        RddResult joinResult = new RddResult();
//
//        BasicOperator<JavaRDD<List<String>>> wayOne1 = new FileSource();
//        String[] wayOne1Args = {args[1]};
//        ArgsUtil.parseArgs(wayOne1, wayOne1Args);
//        ParamsModel<JavaRDD<List<String>>> wayOne1InputArgs = new ParamsModel<>(wayOne1, null);
//        wayOne1InputArgs.setFunctionClasspath(functionPath);
//        wayOne1.execute(wayOne1InputArgs, tableOneResult);
//
//        BasicOperator<JavaRDD<List<String>>> wayOne2 = new FilterOperator();
//        String[] wayOne2Args = {args[2]};
//        ArgsUtil.parseArgs(wayOne2, wayOne2Args);
//        ParamsModel<JavaRDD<List<String>>> wayOne2InputArgs = new ParamsModel<>(wayOne2, null);
//        wayOne2InputArgs.setFunctionClasspath(functionPath);
//        wayOne2.execute(wayOne2InputArgs, tableOneResult);
//
//        BasicOperator<JavaRDD<List<String>>> wayTwo1 = new FileSource();
//        String[] wayTwo1Args = {args[3]};
//        ArgsUtil.parseArgs(wayTwo1, wayTwo1Args);
//        ParamsModel<JavaRDD<List<String>>> wayTwo1InputArgs = new ParamsModel<>(wayTwo1, null);
//        wayTwo1InputArgs.setFunctionClasspath(functionPath);
//        wayTwo1.execute(wayTwo1InputArgs, tableTwoResult);
//
//        BasicOperator<JavaRDD<List<String>>> wayTwo2 = new FilterOperator();
//        String[] wayTwo2Args = {args[4]};
//        ArgsUtil.parseArgs(wayTwo2, wayTwo2Args);
//        ParamsModel<JavaRDD<List<String>>> wayTwo2InputArgs = new ParamsModel<>(wayTwo2, null);
//        wayTwo2InputArgs.setFunctionClasspath(functionPath);
//        wayTwo2.execute(wayTwo2InputArgs, tableTwoResult);
//
//        BinaryBasicOperator<JavaRDD<List<String>>> joinOpt = new JoinOperator();
//        String[] joinArgs = {args[5], args[6], args[7], args[8]};
//        ArgsUtil.parseArgs(joinOpt, joinArgs);
//        BiOptParamsModel<JavaRDD<List<String>>> joinInputArgs = new BiOptParamsModel<>(joinOpt, null);
//        joinInputArgs.setFunctionClasspath(functionPath);
//        joinOpt.execute(joinInputArgs, tableOneResult, tableTwoResult, joinResult);
//
//        BasicOperator<JavaRDD<List<String>>> joinSink = new FileSink();
//        String[] joinSinkArgs = {args[9]};
//        ArgsUtil.parseArgs(joinSink, joinSinkArgs);
//        ParamsModel<JavaRDD<List<String>>> joinSinkInputArgs = new ParamsModel<>(joinSink, null);
//        joinSinkInputArgs.setFunctionClasspath(functionPath);
//        joinSink.execute(joinSinkInputArgs, joinResult);
    }
}
