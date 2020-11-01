package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
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
public class FileSink  extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {
//    // 输入路径
//    @Parameter(names = {"--output"}, required = true)
//    String outputFileName;
//
//    // 输出的分隔符
//    @Parameter(names = {"--sep"})
//    String separateStr = ",";


    public FileSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkFileSink", id, inputKeys, outputKeys, params);
    }

    private FileSystem getFileSystem(String hdfsPath) throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","1");
        //return FileSystem.get(configuration);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return FileSystem.get(new URI(hdfsPath), configuration);
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

        boolean isCombined = this.params.get("isCombined").equals("true"); // todo 之后会根据数据类型在外面自动转换

        if (isCombined) {
            // 针对小文件
            try {
                String outputPath = this.params.get("outputPath");
                String hdfsURI = "hdfs://" + outputPath.split("/")[2];
                Path path = new Path(outputPath);
                FSDataOutputStream fsDataOutputStream = getFileSystem(hdfsURI).create(path);

                this.getInputData("data")
                        .collect()
                        .forEach(record -> {
                            StringBuilder writeLine = new StringBuilder();
                            record.forEach(field -> {
                                writeLine.append(field);
                                writeLine.append(this.params.get("separator"));
                            });
                            writeLine.deleteCharAt(writeLine.length() - 1);
                            writeLine.append("\n");
                            try {
                                fsDataOutputStream.write(writeLine.toString().getBytes("UTF-8"));
//                            fsDataOutputStream.write("\n".getBytes("UTF-8"));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
            } catch ( Exception e) {
                e.printStackTrace();
            }
            // 为了方便其他的节点交互，提供将所有Partition写入一个文件的可能性

        } else {
            // 一个partition写入一个文件
            // 针对大文件
            this.getInputData("data")
                    .map(line -> StringUtils.join(line, this.params.get("separator")))
                    .saveAsTextFile(this.params.get("outputPath"));
        }

    }
}
