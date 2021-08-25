package fdu.daslab.gatewaycenter.utils;

import fdu.daslab.thrift.base.Operator;
import fdu.daslab.thrift.base.PlanNode;
import org.json.JSONArray;


import fdu.daslab.thrift.base.Plan;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zjchen
 * @time 2021/6/15 2:22 下午
 * @description 解析构建plan的Json字符串
 */

@Component
public class PlanBuilder {
    public Plan parseJson(String jsonString){
        Plan plan = new Plan();
        JSONObject planObject = new JSONObject(jsonString);
        setPlanParamsFromJsonArray(plan, planObject.getJSONArray("planParams"));
        JSONArray optArray = planObject.getJSONArray("operators");

        for(int i = 0; i < optArray.length(); i++){
            JSONObject nodeInfo = optArray.getJSONObject(i);
            // getPlanNodeFromJsonObj用来解析operators里面的节点
            PlanNode planNode = getPlanNodeFromJsonObj(nodeInfo);
            plan.nodes.put(planNode.nodeId, planNode);
            if(planNode.inputNodeId.size() == 0) plan.sourceNodes.add(planNode.nodeId);
        }
        setOutputNode(plan);
        return plan;
    }

    private void setPlanParamsFromJsonArray(Plan plan, JSONArray jsonArray){
        for(int i = 0; i < jsonArray.length(); i++){
            JSONObject param = jsonArray.getJSONObject(i);
            plan.others.put(param.getString("name"), param.getString("value"));
        }
    }

    private void setOutputNode(Plan plan){
        plan.nodes.forEach((nodeId, operator)->{
            operator.inputNodeId.forEach((integer -> {
                plan.nodes.get(integer).outputNodeId.add(nodeId);
            }));
        });
    }

    private PlanNode getPlanNodeFromJsonObj(JSONObject nodeInfo){
        int nodeId = nodeInfo.getInt("Id");
        String platformName = nodeInfo.getString("Platform");
        String operatorName = nodeInfo.getString("Name");

        List<Integer> inputNodeId = new ArrayList<>();
        List<Integer> outputNodeId = new ArrayList<>();

        // 解析operator
        Operator operator = new Operator();
        List<String> inputKeys = new ArrayList<>();
        HashMap<String, String> paramMap = new HashMap<>();


        JSONArray operatorParams = nodeInfo.getJSONArray("Params");
        for(int i = 0; i < operatorParams.length(); i++){
            JSONObject param = operatorParams.getJSONObject(i);
            paramMap.put(param.getString("name"), param.getString("value"));
        }

        if (nodeInfo.has("inputKeys")){
            JSONArray inputKeyList = nodeInfo.getJSONArray("inputKeys");
            for(int i = 0; i < inputKeyList.length(); i++) {
                JSONObject inputKey = inputKeyList.getJSONObject(i);
                inputNodeId.add(inputKey.getInt("from"));
                inputKeys.add(inputKey.getString("name"));
            }
        }
        List<String> outputKeys = nodeInfo.getJSONArray("outputKeys").toList().stream().
                map(x->(String)x).
                collect(Collectors.toList());


        operator.setName(operatorName);
        operator.setParams(paramMap);
        operator.setInputKeys(inputKeys);
        operator.setOutputKeys(outputKeys);

        return new PlanNode(nodeId, operator, inputNodeId, outputNodeId, platformName);
    }
}
