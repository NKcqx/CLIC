package siamese;

import adapters.ArgoAdapter;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import channel.Channel;
import fdu.daslab.backend.executor.utils.YamlUtil;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultListenableGraph;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * 与Siamese组的项目对接
 * 包名siamese取自他们的项目文件名
 *
 * 这个类功能是：
 * CLIC将sql语句发送给Siamese，Siamese返回一个树型LogicalPlan
 * 但要注意的是，CLIC也要先让Siamese读取一下每一个table
 * 让Siamese获取到每一个table的schema
 *
 * 他们还没有将他们的项目打包成可以给我们用的包
 * 因此这里只能暂时用Spark SQL代替Siamese
 * 这里类里要进行一些与Spark平台的TableSource算子相似的操作
 * 按理来说CLIC的逻辑计划不能与具体的物理平台混淆
 * 但现在要对接，没办法
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/18 9:00 pm
 */
public class SiameseAdapter {

    // 为了优化SQL，需要启动SparkSession
    // 为了对接没办法
    private static SparkSession sparkSession = SparkInitUtil.getDefaultSparkSession();

    // table名与table地址的映射表
    private static Map<String, String> tableMap = new HashMap<>();

    // 树节点与DAG节点的映射表
    private static Map<LogicalPlan, Operator> optMap = new HashMap<>();

    // 根据Siamese返回的树，我们创建一个DAG
    private static DefaultListenableGraph<Operator, Channel> sqlGraph =
            new DefaultListenableGraph<>(new SimpleDirectedWeightedGraph<>(Channel.class));

    /**
     * CLIC将sql语句发送给Siamese，Siamese返回一个树型LogicalPlan
     * Siamese组尚未将他们项目封装成我们可用的包，因此这里暂时用Spark SQL代替Siamese
     * 逻辑计划与物理平台混淆肯定不行，但现在为了对接没办法
     * @param sqlText
     * @return
     */
    public static void sqlToLogicalPlan(String sqlText) throws Exception {
        LogicalPlan logical = sparkSession.sessionState().sqlParser().parsePlan(sqlText);
        LogicalPlan analyzed = sparkSession.sessionState().analyzer().executeAndCheck(logical);
        LogicalPlan withCachedData = sparkSession.sharedState().cacheManager().useCachedData(analyzed);
        LogicalPlan optimizedPlan = sparkSession.sessionState().optimizer().execute(withCachedData);
        preorderTraversal(optimizedPlan);
        sqlGraphToYaml(sqlGraph);
    }

    /**
     * 要Siamese提供优化SQL的功能，需要先读取table获取这些表的schema
     * 再将schema保存到SparkSession中（将table注册到SparkSession中）
     */
    public static void readTableToGetSchema(String tableAddr) {
        /**
         * 这里实际上先进行跟后面物理平台的TableSource.java算子类似的操作
         * 为了对接没办法
         */
        Dataset<Row> df = null;
        String tableName = "";
        String fileType = "";

        String inputPath = tableAddr;
        if (inputPath.contains(".")) {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1, inputPath.lastIndexOf("."));
            fileType = inputPath.substring(inputPath.lastIndexOf("."), inputPath.length());
        } else {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1);
        }
        // 给之后从Siamese得到的LogicalPlan树生成的DAG使用
        tableMap.put(tableName, inputPath);
        switch (fileType) {
            case ".txt":
                df = sparkSession.read().option("header", "true").csv(inputPath);
                break;
            case ".json":
                df = sparkSession.read().json(inputPath).toDF();
                break;
            default:
                // 默认以csv方式打开数据源文件
                // 如果源文件没有后缀，则按HDFS分布式存储来处理
                df = sparkSession.read().format("csv").option("header", "true").load(inputPath);
        }
        try {
            // 将table注册到SparkSession中
            df.createTempView(tableName);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    /**
     * 遍历Siamese返回的树
     * 为了构建DAG，使用层次遍历
     * @param node
     */
    private static void preorderTraversal(LogicalPlan node) throws Exception {
        if (node == null) {
            return;
        }
        Queue<LogicalPlan> q = new ArrayDeque<LogicalPlan>();
        Operator curOpt = null;
        Operator nextOpt = null;
        q.add(node);
        LogicalPlan cur;
        while (!q.isEmpty()) {
            cur = q.peek();
            if (!optMap.containsKey(cur)) {
                curOpt = nodeToOperator(cur);
                optMap.put(cur, curOpt);
                sqlGraph.addVertex(curOpt);
            }
            // Siamese提供的子节点集合的类型是Scala的seq，需要转换成Java的List
            List<LogicalPlan> childList = scala.collection.JavaConversions.seqAsJavaList(cur.children());
            for (LogicalPlan child : childList) {
                q.add(child);
                if (!optMap.containsKey(child)) {
                    nextOpt = nodeToOperator(child);
                    optMap.put(child, nextOpt);
                    sqlGraph.addVertex(nextOpt);
                }
                sqlGraph.addEdge(optMap.get(child), optMap.get(cur));
            }
            q.poll();
        }
    }

    /**
     * 将树的节点转成CLIC的Operator
     * 为什么要新建一个TFilter而不使用原来的Filter，这是因为在后面具体的物理平台中
     * 比如Spark，普通的批处理算子传递的是RDD类型，而SQL操作算子传递的是Dataset<Row>类型（DataFrame）
     * 因此在同一个平台里实际需要两套算子
     * 为了对接没办法
     *
     * 也可以逻辑层面使用相同的Filter算子，到具体的物理层面再映射成不同的Filter
     * 但这样需要在ArgsUtil.java中进行硬编码判断
     * @param node
     */
    private static Operator nodeToOperator(LogicalPlan node) throws Exception {
        Operator opt = null;
        if (node.getClass().equals(LogicalRelation.class)) {
            opt = OperatorFactory.createOperator("t-relation");
            opt.setParamValue("schema", node.schema().toString());
            // Siamese组还没有解决表名与Relation节点的映射问题
            opt.setParamValue("tableName", "student");
            opt.setParamValue("inputPath", tableMap.get("student"));
        }
        if (node.getClass().equals(Filter.class)) {
            opt = OperatorFactory.createOperator("t-filter");
            opt.setParamValue("schema", node.schema().toString());
            opt.setParamValue("condition", ((Filter) node).condition().toString());
        }
        if (node.getClass().equals(Join.class)) {
            opt = OperatorFactory.createOperator("t-join");
            opt.setParamValue("schema", node.schema().toString());
            opt.setParamValue("condition", ((Join) node).condition().toString());
        }
        if (node.getClass().equals(Project.class)) {
            opt = OperatorFactory.createOperator("t-project");
            opt.setParamValue("schema", node.schema().toString());
        }
        return opt;
    }

    /**
     * 直接把ArgoAdapter的setArgoNode后半部分抄过来
     * @param graph
     */
    private static void sqlGraphToYaml(Graph<Operator, Channel> graph) {
        Map<String, Object> yamlMap = ArgoAdapter.graph2Yaml(graph);
        try {
            String path = YamlUtil.getResPltDagPath() + "physical-dag-" + "1111" + ".yml";
            YamlUtil.writeYaml(new OutputStreamWriter((new FileOutputStream(path))), yamlMap);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
