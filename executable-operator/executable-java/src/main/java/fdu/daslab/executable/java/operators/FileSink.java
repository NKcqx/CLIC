package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * 文件写入
 */
@Parameters(separators = "=")
public class FileSink implements BasicOperator<Stream<List<String>>> {

    // 输入路径
    @Parameter(names={"--output"}, required = true)
    String outputFileName;

    // 输出的分隔符
    @Parameter(names={"--separator"})
    String separateStr = ",";

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        try {
            FileWriter fileWritter = new FileWriter(fileSinkArgs.outputFileName,true);
            BufferedWriter out = new BufferedWriter(fileWritter);
            result.getInnerResult()
                    .forEach(record -> {
                        StringBuilder writeLine = new StringBuilder();
                        record.forEach(field -> {
                            writeLine.append(field);
                            writeLine.append(fileSinkArgs.separateStr);
                        });
                        writeLine.deleteCharAt(writeLine.length() - 1);
                        writeLine.append("\n");
                        try {
                            out.write(writeLine.toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
            out.close();
            fileWritter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
