package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

/**
 * 写入文件的算子
 *
 * @author 唐志伟
 * @since 2020/7/6 1:52 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class FileSink implements BasicOperator<JavaRDD<List<String>>> {
    // 输入路径
    @Parameter(names = {"--output"}, required = true)
    String outputFileName;

    // 输出的分隔符
    @Parameter(names = {"--separator"})
    String separateStr = ",";

    // 是否输出一个文件
    @Parameter(names = {"--isCombined"})
    Boolean isCombined = Boolean.TRUE;

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        // 写入文件
        if (isCombined) {
            // 为了方便其他的节点交互，提供写入一个文件的可能性
            result.getInnerResult()
                .foreachPartition(partitionIter -> {
                    // 所有数据均追加到一个文件上
                    FileWriter fileWritter = new FileWriter(fileSinkArgs.outputFileName, true);
                    PrintWriter out = new PrintWriter(fileWritter);
                    partitionIter.forEachRemaining(record -> {
                        StringBuilder writeLine = new StringBuilder();
                        record.forEach(field -> {
                            writeLine.append(field);
                            writeLine.append(fileSinkArgs.separateStr);
                        });
                        writeLine.deleteCharAt(writeLine.length() - 1);
                        out.println(writeLine);
                        out.flush();
                    });
                    out.close();
                    fileWritter.close();
                });
        } else {
            // 一个partition写入一个文件
            result.getInnerResult()
                .map(line -> StringUtils.join(line, fileSinkArgs.separateStr))
                .saveAsTextFile(fileSinkArgs.outputFileName);
        }

    }
}
