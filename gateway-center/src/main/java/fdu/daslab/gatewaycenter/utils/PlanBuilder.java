package fdu.daslab.gatewaycenter.utils;

import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.PlanNode;
import org.json.JSONArray;


import fdu.daslab.thrift.base.Plan;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zjchen
 * @time 2021/6/15 2:22 下午
 * @description 解析构建plan的Json字符串
 */

public class PlanBuilder {


    public Plan parseJson(String jsonString){
        Plan plan = new Plan();
        JSONObject planObject = new JSONObject(jsonString);
        JSONArray nodeArray = planObject.getJSONArray("nodes");
        for(int i = 0; i < nodeArray.length(); i++){
            JSONObject nodeInfo = nodeArray.getJSONObject(i);
            PlanNode planNode = getPlanNodeFromJsonObj(nodeInfo);
            plan.nodes.put(planNode.nodeId, planNode);
            if(planNode.inputNodeId.size() == 0) plan.sourceNodes.add(planNode.nodeId);
        }
        return plan;
    }

    private PlanNode getPlanNodeFromJsonObj(JSONObject nodeInfo){
        int nodeId = nodeInfo.getInt("nodeId");
        List<Integer> inputNodeId = new ArrayList<>();
        List<Integer> outputNodeId = new ArrayList<>();
        if (nodeInfo.has("inputNodeId")){ inputNodeId = nodeInfo.getJSONArray("inputNodeId").toList().stream().
                    map(x->(Integer)x).
                    collect(Collectors.toList()); }
        if (nodeInfo.has("outputNodeId")){ outputNodeId = nodeInfo.getJSONArray("outputNodeId").toList().stream().
                map(x->(Integer)x).
                collect(Collectors.toList());}
        String platformName = nodeInfo.getString("platformName");
        Operator operator = getOptFromJsonObj(nodeInfo.getJSONObject("operatorInfo"));
        return new PlanNode(nodeId, operator, inputNodeId, outputNodeId, platformName);
    }


    private Operator getOptFromJsonObj(JSONObject operatorInfo){
        Operator operator = new Operator();
        HashMap<String, String> paramMap = new HashMap<>();
        String operatorName = operatorInfo.getString("operatorName");
        JSONArray operatorParams = operatorInfo.getJSONArray("operatorParams");
        List<String> inputKeys = operatorInfo.getJSONArray("inputKeys").toList().stream().
                map(x->(String)x).
                collect(Collectors.toList());
        List<String> outputKeys = operatorInfo.getJSONArray("outputKeys").toList().stream().
                map(x->(String)x).
                collect(Collectors.toList());
        for(int i = 0; i < operatorParams.length(); i++){
            JSONObject param = operatorParams.getJSONObject(i);
            paramMap.put(param.getString("name"), param.getString("value"));
        }

        operator.setName(operatorName);
        operator.setInputKeys(inputKeys);
        operator.setOutputKeys(outputKeys);
        operator.setParams(paramMap);
        return operator;
    }
}
