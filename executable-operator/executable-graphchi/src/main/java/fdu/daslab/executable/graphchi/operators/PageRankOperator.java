package fdu.daslab.executable.graphchi.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.graphchi.algorithm.GraphchiPageRankImp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/23 19:11
 */

public class PageRankOperator extends OperatorBase<InputStream, Stream<List<String>>> {
    Logger logger = LoggerFactory.getLogger(PageRankOperator.class);

    public PageRankOperator(String id, List<String> inputKeys,
                            List<String> outputKeys, Map<String, String> params) {
        super("GraphchiPageRank", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        try {
            //在algorithm中编写pagerank的具体实现
            GraphchiPageRankImp graphchiPageRankImp = new GraphchiPageRankImp();
//
            Random random = new Random();
            String tempFilePath = String.format("%s%s%04x-%04x-%04x-%04x.tmp", "data/tmp/out", File.separator,
                    random.nextInt() & 0xFFFF,
                    random.nextInt() & 0xFFFF,
                    random.nextInt() & 0xFFFF,
                    random.nextInt() & 0xFFFF
            );
            final File tempFile = new File(tempFilePath);
            try {
                FileOutputStream fos = new FileOutputStream(tempFile);
            } catch (IOException e) {
                throw e;
            }
            tempFile.deleteOnExit();
            String graphName = tempFile.toString();

            final InputStream inputStream = this.getInputData("data");
            //调用
            List<List<String>> res = graphchiPageRankImp.exec(inputStream, graphName,
                    Integer.parseInt(this.params.get("shardNum")), Integer.parseInt(this.params.get("iterNum")));
            this.setOutputData("result", res.stream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
