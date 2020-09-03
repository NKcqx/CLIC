package fdu.daslab.executable.java.model;

import fdu.daslab.executable.basic.model.ResultModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 将JavaStream封装成一个ResultMode
 *
 * @author 唐志伟，陈齐翔
 * @since 2020/7/6 1:40 PM
 * @version 1.0
 */
public class StreamResult<Type> implements ResultModel<Stream<Type>> {

    // 内部轮转的java stream
    private Map<String, Stream<Type>> innerStreamMap = new HashMap<>();

    @Override
    public void setInnerResult(String key, Stream<Type> result) {
        innerStreamMap.put(key, result);
        // this.innerStream = result;
    }

    @Override
    public Stream<Type> getInnerResult(String key) {
        return innerStreamMap.get(key);
    }

}
