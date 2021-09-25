/*****************************************************************************
*  参数解析的工具类
*
*  @author   xxm
*  @version  1.0
*
*****************************************************************************/
#ifndef ARGS_UTIL_HPP
#define ARGS_UTIL_HPP

#include <iostream>
#include <fstream>
#include <yaml-cpp/yaml.h>
#include "../model/OperatorBase.hpp"

namespace clic {
    namespace ArgsUtil {
        void parseEdge(YAML::Node &edgeList, vector<OperatorBase*> &heads, map<string, OperatorBase*> &optPool) {
            // if(edgeList.IsSequence()) {
                for(int i = 0; i < edgeList.size(); i++) {
                    YAML::Node edge = edgeList[i];
                    // if(edge.IsMap()) {
                        string curId = edge["id"].as<std::string>();
                        OperatorBase* curOpt = optPool[curId];
                        
                        // 查找是否存在 dependencies 字段
                        YAML::const_iterator iter;
                        if(edge.IsMap()) {
                            for(iter = edge.begin(); iter != edge.end(); ++iter) {
                                string key = iter -> first.as<std::string>();
                                if(key == "dependencies")
                                    break;
                            }
                        }

                        if((!edge.IsMap()) || iter == edge.end()) {    // 不包含 dependencies 字段
                            heads.push_back(curOpt);
                        } else {
                            YAML::Node dependencies = edge["dependencies"];
                            // if(dependencies.IsSequence()) {
                                for(int j = 0; j < dependencies.size(); j++) {
                                    YAML::Node dependency = dependencies[j];
                                    string sourceId = dependency["id"].as<std::string>();
                                    OperatorBase* sourceOpt = optPool[sourceId];
                                    string sourceKey = dependency["sourceKey"].as<std::string>();
                                    string targetKey = dependency["targetKey"].as<std::string>();
                                    sourceOpt -> connectTo(sourceKey, curOpt, targetKey);
                                    curOpt -> connectFrom(targetKey, sourceOpt, sourceKey);
                                }
                            // }
                        }
                    // }
                }
            // }
        }

        void parseOperator(YAML::Node &operatorNodeList, OperatorFactory &factory, map<string, OperatorBase*> &optPool) {
            for(int i = 0; i < operatorNodeList.size(); i++) {
                YAML::Node operatorNode = operatorNodeList[i];
                string optId = operatorNode["id"].as<std::string>();
                string optName = operatorNode["name"].as<std::string>();
                vector<string> inputKeys = operatorNode["inputKeys"].as<std::vector<std::string>>();
                vector<string> outputKeys = operatorNode["outputKeys"].as<std::vector<std::string>>();
                map<string, string> params = operatorNode["params"].as<std::map<std::string, std::string>>();

                OperatorBase* newOpt = factory.createOperator(optName, optId, inputKeys, outputKeys, params);
                optPool[optId] = newOpt;
            }
        }

        void parse(string &filePath, vector<OperatorBase*> &heads, OperatorFactory &factory) {
            std::ifstream file(filePath);
            YAML::Node node = YAML::Load(file);
            YAML::Node operatorNode = node["operators"];
            YAML::Node edgeNode = node["dag"];
            map<string, OperatorBase*> optPool;
            parseOperator(operatorNode, factory, optPool);
            parseEdge(edgeNode, heads, optPool);
            file.close();
        }
    }
}

#endif