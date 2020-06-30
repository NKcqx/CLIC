package fdu.daslab.executable.java.model;

import fdu.daslab.executable.basic.model.ResultModel;

import java.util.List;
import java.util.stream.Stream;

/**
 * 封装JavaStream
 *
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
