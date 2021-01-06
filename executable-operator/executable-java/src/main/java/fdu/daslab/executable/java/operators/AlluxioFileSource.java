package fdu.daslab.executable.java.operators;

//import com.beust.jcommander.Parameter;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 从Alluxio中读取文件，返回一个二维数组，不指定类型
 *
 * @author 张瑞
 * @version 1.0
 */
@Parameters(separators = "=")
public class AlluxioFileSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {
    Logger logger = LoggerFactory.getLogger(AlluxioFileSource.class);
//    // 输入路径
//    @Parameter(names = {"--input"}, required = true)
//    String inputFileName;
//
//    // 输入的分隔符
//    @Parameter(names = {"--separator"})
//    String separateStr = ",";

    public AlluxioFileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("AlluxioFileSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        System.out.println("inputpath is "+ this.params.get("inputPath"));
        try {
            InstancedConfiguration alluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());
            alluxioConf.set(PropertyKey.MASTER_RPC_ADDRESSES, "10.176.22.206:19998");
            FileSystem fileSystem = FileSystem.Factory.create(alluxioConf);
            AlluxioURI path = new AlluxioURI("/java/"+this.params.get("inputPath"));
            if(fileSystem.exists(path)){
                logger.info("Stage(java) ———— Read from Alluxio" );
            } else {
                logger.info("Stage(java) ———— File doesn't exist in Alluxio");
            }

            FileInStream inStream = fileSystem.openFile(path);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inStream));

            String line;
            List<List<String>> resultList = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                resultList.add(Arrays.asList(line.split(this.params.get("separator"))));
            }
            this.setOutputData("result", resultList.stream());
            // result.setInnerResult("result", resultList.stream()); // 设置最后的stream
            bufferedReader.close();
            inStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
