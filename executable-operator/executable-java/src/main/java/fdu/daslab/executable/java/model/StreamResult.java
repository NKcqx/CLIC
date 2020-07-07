package fdu.daslab.executable.java.model;

import fdu.daslab.executable.basic.model.ResultModel;

import java.util.List;
import java.util.stream.Stream;

/**
 * 将JavaStream封装成一个ResultMode
 *
 * @author 唐志伟
 * @since 2020/7/6 1:40 PM
 * @version 1.0
 */
public class StreamResult implements ResultModel<Stream<List<String>>> {

    // 内部轮转的java stream
    private Stream<List<String>> innerStream;

    @Override
    public void setInnerResult(Stream<List<String>> result) {
        this.innerStream = result;
    }

    @Override
    public Stream<List<String>> getInnerResult() {
        return innerStream;
    }

}
