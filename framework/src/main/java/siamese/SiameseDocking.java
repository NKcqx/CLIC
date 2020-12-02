package siamese;

import basic.operators.Operator;
import channel.Channel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.javatuples.Pair;
import org.jgrapht.Graph;

import java.util.*;

/**
 * 与Siamese组的项目对接
 * 包名siamese取自他们的项目文件名Siamese
 *
 * 这个类功能是：
 * CLIC将sql语句发送给Siamese，Siamese返回一个树型LogicalPlan
 * 但要注意的是，CLIC也要先让Siamese读取一下每一个table
 * 让Siamese获取到每一个table的schema
 *
 * Siamese还没有封装好他们的包给我们用，所以先用真正的Spark SQL代替
 * 本类中要进行一些与Spark平台的TableSource算子相似的操作
 * 按理来说CLIC的逻辑计划不能与具体的物理平台混淆
 * 但现在要对接，没办法
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/18 9:00 pm
 */
public class SiameseDocking {

    // 为了优化SQL，需要启动SparkSession
    // 为了对接没办法
    private static SparkSession sparkSession = SparkInitUtil.getDefaultSparkSession();

    // 树的根节点
    private static LogicalPlan rootNode = null;
    // 树节点与DAG节点的映射表
    private static Map<LogicalPlan, Operator> optMap = new HashMap<>();

    /**
     * 与Siamese的对接，从这里开始
     */
    public static void unfoldQuery2DAG(String sqlText, Operator endOpt, Graph<Operator, Channel> graph) {
        try {
            // CLIC的SQL -> Siamese的树
            rootNode = sql2LogicalPlan(sqlText);
            // Siamese的树 -> CLIC的DAG
            leverOrderTraversal(rootNode, graph);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 将新生成的子DAG的最后一个节点（树的根节点）对应的算子与外面的原query算子的下一跳算子连接
        graph.addEdge(optMap.get(rootNode), endOpt);
    }

    /**
     * CLIC将sql语句发送给Siamese，Siamese返回一个树型LogicalPlan
     * Siamese组尚未将他们项目封装成我们可用的包，因此这里先用真正的Spark SQL代替
     * 逻辑计划与物理平台混淆肯定不行，但现在为了对接没办法
     * @param sqlText
     * @return
     * @throws Exception
     */
    public static LogicalPlan sql2LogicalPlan(String sqlText) throws ParseException {
        LogicalPlan logical = null;
        logical = sparkSession.sessionState().sqlParser().parsePlan(sqlText);
        LogicalPlan analyzed = sparkSession.sessionState().analyzer().executeAndCheck(logical);
        LogicalPlan withCachedData = sparkSession.sharedState().cacheManager().useCachedData(analyzed);
        LogicalPlan optimizedPlan = sparkSession.sessionState().optimizer().execute(withCachedData);
        return optimizedPlan;
    }

    /**
     * 遍历Siamese返回的树
     * 注意Siamese返回给CLIC的是树的根节点，而CLIC的DAG要从树的叶子节点开始构建
     * 即树的边方向与要生成的DAG的边方向相反
     * @param rootNode
     */
    private static void leverOrderTraversal(LogicalPlan rootNode, Graph<Operator, Channel> graph) throws Exception {
        if (rootNode == null) {
            return;
        }
        Queue<LogicalPlan> q = new ArrayDeque<LogicalPlan>();
        Operator curOpt = null;
        q.add(rootNode);
        LogicalPlan cur;
        // 层次遍历
        while (!q.isEmpty()) {
            cur = q.peek();
            if (!optMap.containsKey(cur)) {
                curOpt = node2Operator(cur);
                optMap.put(cur, curOpt);
                graph.addVertex(curOpt);
            }
            // Siamese提供的子节点集合的类型是Scala的seq，需要转换成Java的List
            List<LogicalPlan> childList = scala.collection.JavaConversions.seqAsJavaList(cur.children());
            // 这里需要判断一下是不是join节点
            if (cur.getClass().equals(Join.class)) {
                // 如果当前节点是join节点，则要对DAG边的构建处理一下
                // 因为join算子的targetKey不是默认的"data"，而是"leftTable"或"rightTable"
                // 与左表搭建边
                constructEdgeOfDAG(cur, childList.get(0), q, graph, "leftTable");
                // 与右表搭建边
                constructEdgeOfDAG(cur, childList.get(1), q, graph, "rightTable");
            } else {
                // 如果当前节点是其他类型的节点
                // 遍历当前节点的所有子节点，将它们入队，让新DAG添加子节点和边
                // 其实这里不用for循环，因为这棵树除了join节点，其他节点都只有一个子节点
                for (int i = 0; i<childList.size(); i++) {
                    constructEdgeOfDAG(cur, childList.get(i), q, graph, null);
                }
            }
            q.poll();
        }
    }

    /**
     * 将树的节点转成CLIC的Operator
     * 为什么这里新建的Operator的种类名前带一个'T'
     * 例如新建的是TFilter而不是Filter，这是因为在后面具体的物理平台中
     * 比如Spark，普通的批处理算子传递的是RDD类型，而SQL操作算子传递的是Dataset<Row>类型（DataFrame）
     * 因此在同一个平台里实际需要两套算子
     * 为了对接没办法
     *
     * 也可以在逻辑阶段使用相同的Filter算子，到具体的物理层面再映射成不同的Filter
     * 但这样需要在ArgsUtil.java中用硬编码来辨别这个Filter是传递RDD还是传递Dataset<Row>的Filter
     * @param node
     */
    private static Operator node2Operator(LogicalPlan node) throws Exception {
        // 工厂模式创建Operator
        Operator opt = SiameseOptFactory.createOperator(node);
        if (opt == null) {
            throw new NoSuchMethodException("缺少该种类的SQL算子：" + node.getClass());
        }
        return opt;
    }

    /**
     * 根据树的节点构建DAG的边
     * @param cur 当前节点
     * @param child 当前节点的第i个子节点
     * @param q 层次遍历的队列
     * @param graph 要生成的DAG
     * @param joinDesc 对Join节点需要特别对待
     */
    private static void constructEdgeOfDAG(LogicalPlan cur, LogicalPlan child, Queue<LogicalPlan> q,
                                           Graph<Operator, Channel> graph, String joinDesc) throws Exception {
        Operator nextOpt = null;
        q.add(child);
        if (!optMap.containsKey(child)) {
            nextOpt = node2Operator(child);
            optMap.put(child, nextOpt);
            graph.addVertex(nextOpt);
        }

        if (joinDesc == null) {
            graph.addEdge(optMap.get(child), optMap.get(cur));
        } else if (joinDesc.equals("leftTable") || joinDesc.equals("rightTable")) {
            List<Pair<String, String>> KeyPairs = new ArrayList<>();
            KeyPairs.add(new Pair<>("result", joinDesc));
            Channel Channel = new Channel(KeyPairs);
            graph.addEdge(optMap.get(child), optMap.get(cur), Channel);
        } else {
            // 其实这里不用抛出异常，只是为了让上面的else if分支里joinDesc取哪些值能列出来
            throw new IllegalArgumentException("join的参数不合法！");
        }
    }
}
