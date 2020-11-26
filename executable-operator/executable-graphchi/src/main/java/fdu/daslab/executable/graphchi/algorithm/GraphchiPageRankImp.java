package fdu.daslab.executable.graphchi.algorithm;


import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.io.CompressedIO;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.util.IdFloat;
import edu.cmu.graphchi.vertexdata.ForeachCallback;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/23 20:02
 */
public class GraphchiPageRankImp implements GraphChiProgram<Float, Float> {
    Logger logger = LoggerFactory.getLogger(GraphchiPageRankImp.class);

    public void update(ChiVertex<Float, Float> vertex, GraphChiContext context) {
        if (context.getIteration() == 0) {
            /* Initialize on first iteration */
            vertex.setValue(1.0f);
        } else {
            /* On other iterations, set my value to be the weighted
               average of my in-coming neighbors pageranks.
             */
            float sum = 0.f;
            for (int i = 0; i < vertex.numInEdges(); i++) {
                sum += vertex.inEdge(i).getValue();
            }
            vertex.setValue(0.15f + 0.85f * sum);
        }

        /* Write my value (divided by my out-degree) to my out-edges so neighbors can read it. */
        float outValue = vertex.getValue() / vertex.numOutEdges();
        for (int i = 0; i < vertex.numOutEdges(); i++) {
            vertex.outEdge(i).setValue(outValue);
        }

    }

    public void beginIteration(GraphChiContext ctx) {
    }

    public void endIteration(GraphChiContext ctx) {
    }

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    /**
     * Initialize the sharder-program.
     *
     * @param graphName
     * @param numShards
     * @return
     * @throws IOException
     */
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Float, Float>(graphName, numShards, new VertexProcessor<Float>() {
            public Float receiveVertexValue(int vertexId, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new EdgeProcessor<Float>() {
            public Float receiveEdge(int from, int to, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new FloatConverter(), new FloatConverter());
    }

    /**
     * @param inputStream fileInputStream
     * @param graphName
     * @param shardNum    分片数
     * @param fileType    文件中图结构类型，包括 edgelist和 adjacencylist 两种
     * @param iterNum     pagerank计算迭代数
     * @return pagerank 的结果，形式为降序的 id value
     * @throws Exception
     */
    public List<List<String>> exec(InputStream inputStream, String graphName, int shardNum,
                                   String fileType, int iterNum) throws Exception {

        List<List<String>> res = new ArrayList<>();
        CompressedIO.disableCompression();
        /* Create shards */
        FastSharder sharder = createSharder(graphName, shardNum);
        sharder.shard(inputStream, fileType);

        /* Run GraphChi */
        GraphChiEngine<Float, Float> engine = new GraphChiEngine<Float, Float>(graphName, shardNum);
        engine.setEdataConverter(new FloatConverter());
        engine.setVertexDataConverter(new FloatConverter());
        engine.setModifiesInedges(false); // Important optimization

        engine.run(new GraphchiPageRankImp(), iterNum);
        logger.info("graphchi run");

        /* Output results */
        //为了后面使用trans.backward将graphchi内部使用的vertex id 转换为原始图的vertex id
        VertexIdTranslate trans = engine.getVertexIdTranslate();
        //获取所有的rank值
        TreeSet<IdFloat> resList = new TreeSet<IdFloat>(new IdFloat.Comparator());
        VertexAggregator.foreach(engine.numVertices(), graphName, new FloatConverter(), new ForeachCallback<Float>()  {
                    public void callback(int vertexId, Float vertexValue) {
                        resList.add(new IdFloat(vertexId, vertexValue));
                    }
        });

        for (IdFloat vertexRank : resList) {
            List<String> tmp = new ArrayList<>();
            tmp.add(String.valueOf(trans.backward(vertexRank.getVertexId())));
            tmp.add(String.valueOf(vertexRank.getValue()));
            res.add(tmp);
            logger.info(trans.backward(vertexRank.getVertexId()) + " = " + vertexRank.getValue());
        }
        return res;
    }
}
