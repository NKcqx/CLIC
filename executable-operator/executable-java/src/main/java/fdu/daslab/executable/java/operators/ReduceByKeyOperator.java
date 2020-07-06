package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java平台的reduce函数
 */
@Parameters(separators = "=")
public class ReduceByKeyOperator implements BasicOperator<Stream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String reduceFunctionName;

    // 获取key的function
    @Parameter(names = {"--keyName"})
    String keyExtractFunctionName;

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        ReduceByKeyOperator reduceArgs = (ReduceByKeyOperator) inputArgs.getOperatorParam();
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        @SuppressWarnings("unchecked")
        Map<String, List<String>> reduceMap = result.getInnerResult()
                .collect(Collectors.groupingBy(data -> (String) functionModel.invoke(reduceArgs.keyExtractFunctionName, data),
                        new ReducingCollector<>((data1, data2) ->
                                (List<String>) functionModel.invoke(reduceArgs.reduceFunctionName, data1, data2))));
        result.setInnerResult(reduceMap.values().stream());
    }

    /**
     * 下面的实现照搬rheem
     *
     * @param <T>
     */
    private static class ReducingCollector<T> implements Collector<T, List<T>, T> {

        private final BinaryOperator<T> reduceFunction;

        ReducingCollector(BinaryOperator<T> reduceFunction) {
            this.reduceFunction = reduceFunction;
        }

        @Override
        public Supplier<List<T>> supplier() {
            return () -> new ArrayList<>(1);
        }

        @Override
        public BiConsumer<List<T>, T> accumulator() {
            return (list, element) -> {
                if (list.isEmpty()) {
                    list.add(element);
                } else {
                    list.set(0, this.reduceFunction.apply(list.get(0), element));
                }
            };
        }

        @Override
        public BinaryOperator<List<T>> combiner() {
            return (list1, list2) -> {
                if (list1.isEmpty()) {
                    return list2;
                } else if (list2.isEmpty()) {
                    return list2;
                } else {
                    list1.set(0, this.reduceFunction.apply(list1.get(0), list2.get(0)));
                    return list1;
                }
            };
        }

        @Override
        public Function<List<T>, T> finisher() {
            return list -> {
                assert !list.isEmpty();
                return list.get(0);
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }
}
