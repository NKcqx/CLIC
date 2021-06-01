package fdu.daslab.operatorcenter.service;

import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.operatorcenter.OperatorCenter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 算子中心的服务
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 10:33 AM
 */
@Service
public class OperatorHandler implements OperatorCenter.Iface {

    private static Logger logger = LoggerFactory.getLogger(OperatorCenter.class);

    @Override
    public void addPlatform(Platform platform) throws TException {
        logger.info("add platform!");
    }

    @Override
    public Platform findPlatformInfo(String platformName) throws TException {
        return null;
    }

    @Override
    public void addOperator(Operator operator) throws TException {
        logger.info("add operator!");
    }

    @Override
    public Operator findOperatorInfo(String operatorName, String platformName) throws TException {
        return null;
    }
}
