package fdu.daslab.gatewaycenter.abstractobj;

import fdu.daslab.thrift.base.Operator;

import java.util.HashMap;
import java.util.List;

/**
 * @author zjchen
 * @time 2021/6/15 7:39 下午
 * @description
 */

public class Plan {
    public String planName;

    public HashMap<String, Object> planParams;

    public List<Operator> planEntity;
}
