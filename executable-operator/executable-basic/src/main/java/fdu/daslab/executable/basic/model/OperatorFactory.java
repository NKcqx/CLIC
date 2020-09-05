package fdu.daslab.executable.basic.model;

import java.util.List;
import java.util.Map;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/19 11:52 上午
 */
public interface OperatorFactory {
     OperatorBase createOperator(String name, String id, List<String> inputKeys,
                                              List<String> outputKeys, Map<String, String> params) throws  Exception;
}
