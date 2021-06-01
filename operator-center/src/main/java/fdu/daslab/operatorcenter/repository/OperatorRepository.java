package fdu.daslab.operatorcenter.repository;

import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.Platform;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
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

    private List<Operator> operatorList = new ArrayList<>();
    private List<Platform> platformList = new ArrayList<>();

    public void addPlatform(Platform platform) {
        platformList.add(platform);
    }

    public Platform findPlatformInfo(String platformName) {
        return platformList.stream()
                .filter(platform -> platform.name.equals(platformName))
                .collect(Collectors.toList())
                .get(0);
    }

    public void addOperator(Operator operator) {
        operatorList.add(operator);
    }

    public Operator findOperatorInfo(String operatorName) {
        return operatorList.stream()
                .filter(operator -> operator.name.equals(operatorName))
                .collect(Collectors.toList())
                .get(0);
    }
}
