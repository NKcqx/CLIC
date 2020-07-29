package platform.spark.front;


import api.DataQuanta;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FrontJsonParse {


    //new feature parse JsonFile

    private HashMap<Integer, JSONObject> nodeOrderJSONTable = new HashMap<>();
    private HashMap<Integer, DataQuanta> nodeOrderDataQuanta = new HashMap<>();

    public void parseJson(String jsonFilePath) { //现在前端的json默认存储在浏览器下载路径下
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(jsonFilePath)) {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONObject nodeObject = (JSONObject) obj;
            JSONArray nodeList = (JSONArray) nodeObject.get("nodeList");
            nodeList.forEach(node -> {
                try {
                    parseNodeObject((JSONObject) node);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void parseNodeObject(JSONObject node) throws java.lang.Exception {

        DataQuanta temp;

        Integer nodeOrder = Integer.parseInt((String) node.get("DagOrder"));
        String nodeName = (String) node.get("nodeName");
        String dataPath = (String) node.get("data_path");
        String parquet = (String) node.get("parquet");

        if (nodeOrder == 5) {
            temp = DataQuanta.createInstance(nodeName, null);
        } else {
            temp = DataQuanta.createInstance(nodeName, new HashMap<String, String>() {{
                put(dataPath, parquet);
            }});
        }


        nodeOrderDataQuanta.put(nodeOrder, temp);
        nodeOrderJSONTable.put(nodeOrder, node);
    }

    public void connectDAG() {
        for (Map.Entry<Integer, JSONObject> entry : nodeOrderJSONTable.entrySet()) {
            Integer nodeOrder = entry.getKey();
            DataQuanta node = nodeOrderDataQuanta.get(nodeOrder);
            JSONObject nodeJSON = entry.getValue();
            Integer outId = Integer.parseInt((String) nodeJSON.get("out_id"));
            if (outId > 0) {
                node.outgoing(nodeOrderDataQuanta.get(outId));
            }
        }

    }

    public HashMap<Integer, DataQuanta> getNodeOrderDataQuanta() {
        return this.nodeOrderDataQuanta;
    }


}
