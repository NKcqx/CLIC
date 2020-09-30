package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HDFSSource extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    @Parameter(names = {"--input"}, required = true)
    String inputHdfsPath;

    @Parameter(names = {"--separator"})
    String separator = ",";

    public HDFSSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("HDFSSource", id, inputKeys, outputKeys, params);
    }

    private FileSystem getFileSystem() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","1");
        return FileSystem.get(new URI("hdfs://localhost:8020"),configuration,"zhuxingpo");
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        BufferedReader br = null;
        FSDataInputStream fsDataInputStream = null;
        try {
            Path path = new Path(this.params.get("inputPath"));
            fsDataInputStream = getFileSystem().open(path);
            br = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line = null;
            List<List<String>> resultList = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                resultList.add(Arrays.asList(line.split(this.params.get("separator"))));
            }
            this.setOutputData("result",resultList.stream());

        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                IOUtils.closeStream(br);
            }
            if (fsDataInputStream != null) {
                IOUtils.closeStream(fsDataInputStream);
            }
        }
    }
}