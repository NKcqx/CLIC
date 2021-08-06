package fdu.daslab.operatorcenter.service;

import fdu.daslab.operatorcenter.repository.OperatorRepository;
import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.operatorcenter.OperatorCenter;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * 算子中心的服务
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 10:33 AM
 */
@Service
public class OperatorHandler implements OperatorCenter.Iface {

    @Autowired
    private OperatorRepository operatorRepository;

    @Override
    public void addPlatform(Platform platform) throws TException {
        operatorRepository.addPlatform(platform);
    }

    @Override
    public Platform findPlatformInfo(String platformName) throws TException {
        return operatorRepository.findPlatformInfo(platformName);
    }

    @Override
    public Map<String, Platform> listPlatforms() throws TException {
        return operatorRepository.listPlatforms();
    }


    @Override
    public void addOperator(Operator operator) throws TException {
        operatorRepository.addOperator(operator);
    }

    @Override
    public Operator findOperatorInfo(String operatorName, String platformName) throws TException {
        return operatorRepository.findOperatorInfo(operatorName);
    }
}
