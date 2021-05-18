package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Flink流处理，先keyBy, 然后调用底层process函数，实现功能：注册窗口结束定时器，每到窗口的末尾就输出TOPN结果
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/31 21:51
 */

// 测试用，先不删
public class StreamTopNByKeyInWindow extends OperatorBase<DataStream<List<String>>, DataStream<String>> {
    public StreamTopNByKeyInWindow(String id,
                                   List<String> inputKeys,
                                   List<String> outputKeys,
                                   Map<String, String> params) {
        super("StreamFlinkTopNByKeyInWindow", id, inputKeys, outputKeys, params);
    }

    @Override
    // QUESTION: 如果输入数据大小太大，会报java.lang.OutOfMemoryError: Java heap space
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataStream<String>> result) {
        // ReduceByKeyOperator reduceArgs = (ReduceByKeyOperator) inputArgs.getOperatorParam();
        final String windowEnd = this.params.get("windowEnd");
        final String keyName = this.params.get("keyName");
        final String topSize = this.params.get("topSize");
        final String id = this.params.get("id");
        final DataStream<String> resultStream = this.getInputData("data")
                .keyBy((KeySelector<List<String>, String>) data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (String) functionModel.invoke(windowEnd, data);
                })
                .process(new KeyedProcessFunction<String, List<String>, String>() {
                    // 定义列表状态，保存当前窗口所有的数据
                    ListState<List<String>> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("lists", TypeInformation.of(new TypeHint<List<String>>() {
                        })));
                    }

                    @Override
                    public void processElement(List<String> data, Context context, Collector<String> collector) throws Exception {
                        // 每来一条数据，存入ListState,并注册定时器
                        listState.add(data);
                        FunctionModel functionModel = inputArgs.getFunctionModel();
                        Long we = Long.parseLong((String) functionModel.invoke(windowEnd, data));
                        context.timerService().registerEventTimeTimer(we+1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发，当前已收集的所有数据，排序输出
//                        ArrayList<List<String>> lists = Lists.newArrayList(listState.get().iterator());
                        List<List<String>> lists = IteratorUtils.toList(listState.get().iterator());
                        lists.sort(new Comparator<List<String>>() {
                            @Override
                            public int compare(List<String> o1, List<String> o2) {
                                FunctionModel functionModel = inputArgs.getFunctionModel();
                                Integer i1 = Integer.parseInt((String) functionModel.invoke(keyName, o1));
                                Integer i2 = Integer.parseInt((String) functionModel.invoke(keyName, o2));
                                return i2-i1;
                            }
                        });


                        // 将排名信息格式化为字符串,
                        // todo: 目前为了展示所以做成了控制台输出，后面改成文件输出
                        FunctionModel functionModel = inputArgs.getFunctionModel();
                        StringBuilder resultBuilder = new StringBuilder();
                        resultBuilder.append("=====================================\n");
                        resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append('\n');

//                        final List<String> stringList = Collections.singletonList(new Timestamp(timestamp - 1).toString());
                        for(int i = 1; i <= Math.min(Integer.parseInt(topSize), lists.size()); i++){
                            List<String> currentListString = lists.get(i - 1);
                            final String curId = (String) functionModel.invoke(id, currentListString);
                            final String curCnt = (String) functionModel.invoke(keyName, currentListString);
//                            stringList.add(curId);
//                            stringList.add(curCnt);
                            resultBuilder.append("ID: ").append(i).append(":")
                                    .append(" 商品ID: ").append(curId)
                                    .append(" 热门度: ").append(curCnt)
                                    .append('\n');
                        }
                        resultBuilder.append("=====================================\n\n");

                        // 控制输出频率
                        Thread.sleep(1000L);

                        out.collect(resultBuilder.toString());
                    }
                    
//                    @Override
//                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//                        // 定时器触发，当前已收集的所有数据，排序输出
//                        ArrayList<List<String>> lists = Lists.newArrayList(listState.get().iterator());
//                        lists.sort(new Comparator<List<String>>() {
//                            @Override
//                            public int compare(List<String> o1, List<String> o2) {
//                                FunctionModel functionModel = inputArgs.getFunctionModel();
//                                Integer i1 = Integer.parseInt((String) functionModel.invoke(keyName, o1));
//                                Integer i2 = Integer.parseInt((String) functionModel.invoke(keyName, o2));
//                                return i2-i1;
//                            }
//                        });
//
//
//                        // 将排名信息格式化为字符串
//                        FunctionModel functionModel = inputArgs.getFunctionModel();
//                        StringBuilder resultBuilder = new StringBuilder();
//                        resultBuilder.append("=====================================\n");
//                        resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append('\n');
//                        for(int i = 1; i <= Math.min(Integer.parseInt(topSize), lists.size()); i++){
//                            List<String> currentListString = lists.get(i - 1);
//                            final String curId = (String) functionModel.invoke(id, currentListString);
//                            final String curCnt = (String) functionModel.invoke(keyName, currentListString);
//                            resultBuilder.append("ID: ").append(i).append(":")
//                                    .append(" 商品ID: ").append(curId)
//                                    .append(" 热门度: ").append(curCnt)
//                                    .append('\n');
//                        }
//                        resultBuilder.append("=====================================\n\n");
//
//                        // 控制输出频率
//                        Thread.sleep(1000L);
//
//                        out.collect(resultBuilder.toString());
//                    }
                });

//        resultStream.print();

//        final StreamExecutionEnvironment fsEnv = resultStream.getExecutionEnvironment();
//        try {
//            fsEnv.execute();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
        this.setOutputData("result", resultStream);
    }
}
