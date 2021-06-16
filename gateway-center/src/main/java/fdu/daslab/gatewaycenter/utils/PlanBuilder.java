package fdu.daslab.gatewaycenter.utils;

import com.google.gson.Gson;
import fdu.daslab.thrift.base.Plan;

/**
 * @author zjchen
 * @time 2021/6/15 2:22 下午
 * @description
 */

public class PlanBuilder {

    public void parseJson(String jsonString){
        Gson gson = new Gson();
        Plan plan = gson.fromJson(jsonString, Plan.class);

    }

}
