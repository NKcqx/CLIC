package fdu.daslab.executable.java.operators;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 文件写入的算子
 *
 * @author 张瑞
 * @version 1.0
 */
@Parameters(separators = "=")
public class AlluxioFileSink extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {
    Logger logger = LoggerFactory.getLogger(FileSource.class);
    // 输入路径
    @Parameter(names = {"--output"}, required = true)
    String outputFileName;

    // 输出的分隔符
    @Parameter(names = {"--separator"})
    String separateStr = ",";

    public AlluxioFileSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("AlluxioFileSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        // FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        try {
            InstancedConfiguration alluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());
            alluxioConf.set(PropertyKey.MASTER_RPC_ADDRESSES, "10.176.24.160:19998");
            FileSystem fileSystem = FileSystem.Factory.create(alluxioConf);
            AlluxioURI path = new AlluxioURI("/java/" + this.params.get("outputPath"));
            if (fileSystem.exists(path)) {
                logger.info("Stage(java) ———— Write to Alluxio");
            } else {
                logger.info("Stage(java) ———— File doesn't exist in Alluxio");
            }
            FileOutStream outStream = fileSystem.createFile(path);

            this.getInputData("data")
                // result.getInnerResult("data")
                .forEach(record -> {
                    StringBuilder writeLine = new StringBuilder();
                    record.forEach(field -> {
                        writeLine.append(field);
                        writeLine.append(this.params.get("separator"));
                    });
                    writeLine.deleteCharAt(writeLine.length() - 1);
                    writeLine.append("\n");
                    try {
                        outStream.write(writeLine.toString().getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            outStream.close();
            // 数据准备好
            this.getMasterClient().postDataPrepared();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
