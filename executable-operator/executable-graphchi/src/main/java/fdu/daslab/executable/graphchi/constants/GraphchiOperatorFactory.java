package fdu.daslab.executable.graphchi.constants;

import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.graphchi.operators.*;

import java.util.HashMap;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/23 17:16
 */
public class GraphchiOperatorFactory extends OperatorFactory {

    // 初始化所有的operator的映射关系
    public GraphchiOperatorFactory() {
        operatorMap = new HashMap<String, Class>() {{
            put("PageRankOperator", PageRankOperator.class);
            put("SourceOperator", FileSourceOperator.class);
            put("SinkOperator", FileSinkOperator.class);
        }};
    }
}
