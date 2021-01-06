package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.thrift.TException;
import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

/**
 * 写入文件的算子
 *
 * @author 唐志伟
 * @since 2020/7/6 1:52 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class AlluxioFileSink extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {
//    // 输入路径
//    @Parameter(names = {"--output"}, required = true)
//    String outputFileName;
//
//    // 输出的分隔符
//    @Parameter(names = {"--sep"})
//    String separateStr = ",";


    public AlluxioFileSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkAlluxioFileSink", id, inputKeys, outputKeys, params);
    }


//    // 是否输出一个文件
//    @Parameter(names = {"--isCombined"})
//    Boolean isCombined = Boolean.TRUE;

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        // FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        // 写入文件

//        this.getInputData("data")
//                .map(line -> StringUtils.join(line, this.params.get("separator")))
//                .saveAsTextFile(this.params.get("outputPath"));

        InstancedConfiguration alluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());
        alluxioConf.set(PropertyKey.MASTER_RPC_ADDRESSES, "10.176.24.160:19998");
        FileSystem fileSystem = FileSystem.Factory.create(alluxioConf);


        boolean isCombined = this.params.get("isCombined").equals("true"); // todo 之后会根据数据类型在外面自动转换
        if (isCombined) {
            // 为了方便其他的节点交互，提供将所有Partition写入一个文件的可能性
            this.getInputData("data")
                    .foreachPartition(partitionIter -> {
                        // 所有数据均追加到一个文件上
                        AlluxioURI path = new AlluxioURI("/java/" + this.params.get("outputPath"));
                        FileOutStream outStream = fileSystem.createFile(path);
                        /*FileWriter fileWritter = new FileWriter(this.params.get("outputPath"), true);
                        PrintWriter out = new PrintWriter(fileWritter);*/
                        partitionIter.forEachRemaining(record -> {
                            StringBuilder writeLine = new StringBuilder();
                            record.forEach(field -> {
                                writeLine.append(field);
                                writeLine.append(this.params.get("separator"));
                            });
                            writeLine.deleteCharAt(writeLine.length() - 1);
                            try {
                                outStream.write(writeLine.toString().getBytes());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        });
                        outStream.close();
                    });
        } else {
            // 一个partition写入一个文件
            this.getInputData("data")
                    .map(line -> StringUtils.join(line, this.params.get("separator")))
                    .saveAsTextFile(this.params.get("outputPath"));
            //TODO: 将outputpath更改为alluxio的URI即可。
            // 例子：double.saveAsTextFile("alluxio://localhost:19998/Output")

        }
        try {
            // 数据准备好
            this.getMasterClient().postDataPrepared();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
