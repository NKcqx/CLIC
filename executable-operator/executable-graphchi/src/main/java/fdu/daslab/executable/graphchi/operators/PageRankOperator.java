package fdu.daslab.executable.graphchi.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.graphchi.algorithm.GraphchiPageRankImp;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/23 19:11
 */

public class PageRankOperator extends OperatorBase<InputStream, Stream<List<String>>> {
    public PageRankOperator(String id, List<String> inputKeys,
                            List<String> outputKeys, Map<String, String> params) {
        super("GraphchiPageRank", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        try {
            //在algorithm中编写pagerank的具体实现
            GraphchiPageRankImp graphchiPageRankImp = new GraphchiPageRankImp();
            //调用
            List<List<String>> res = graphchiPageRankImp.exec(this.getInputData("data"), this.params.get("graphName"),
                    Integer.parseInt(this.params.get("shardNum")), Integer.parseInt(this.params.get("iterNum")));
            this.setOutputData("result", res.stream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
