package edu.daslab.executable.basic.model;

import java.util.List;
import java.util.stream.Stream;

/**
 * join等需要结合多条stream的基础算子
 */
public class BinaryBasicOperator implements BasicOperator<Stream<List<String>>> {

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> result) {

    }

    public void binaryExecute(ParamsModel<Stream<List<String>>> inputArgs,
                              ResultModel<Stream<List<String>>> result1,
                              ResultModel<Stream<List<String>>> result2) {
    }
}
