#ifndef ARG_UTIL_HPP
#define ARG_UTIL_HPP

#include <iostream>
#include <fstream>
#include <yaml-cpp/yaml.h>
#include "../model/OperatorBase.hpp"

namespace clic {
    class ArgUtil {
        protected:
            map<string, OperatorBase*> optPool;

            void parseEdge(YAML::Node &edgeList, vector<OperatorBase*> &heads) {
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

            // TODO：待 IoC 完成后，OperatorFactory 可完成任意平台算子的创建，此处无需再设成虚函数
            virtual void parseOperator(YAML::Node &operatorNodeList) = 0;
        
        public:
            void parse(string &filePath, vector<OperatorBase*> &heads) {
                std::ifstream file(filePath);
                YAML::Node node = YAML::Load(file);
                YAML::Node operatorNode = node["operators"];
                YAML::Node edgeNode = node["dag"];
                map<string, OperatorBase*> optPool;
                parseOperator(operatorNode);
                parseEdge(edgeNode, heads);
                file.close();
            }

            ~ArgUtil() {
                for(map<string, OperatorBase*>::iterator iter = optPool.begin(); iter != optPool.end(); iter++) {
                    delete iter -> second;
                }
            }
    };
}

#endif