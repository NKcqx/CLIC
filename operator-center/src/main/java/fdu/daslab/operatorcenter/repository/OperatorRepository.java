package fdu.daslab.operatorcenter.repository;

import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.Platform;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 算子和平台的存储和读写，暂时使用内存，后面都需要使用其他的存储方式
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/26/21 4:45 PM
 */
@Repository
public class OperatorRepository {

    // TODO: Operator的设计还有很大的问题，需要考虑如何设计logical 和 physical的
    private Map<String, Operator> operators = new HashMap<>();
    private Map<String, Platform> platforms = new HashMap<>();

    public void addPlatform(Platform platform) {
        platforms.put(platform.name, platform);
    }

    public Platform findPlatformInfo(String platformName) {
        return platforms.get(platformName);
    }

    public void addOperator(Operator operator) {
        operators.put(operator.name, operator);
    }

    public Operator findOperatorInfo(String operatorName) {
        return operators.get(operatorName);
    }
}
