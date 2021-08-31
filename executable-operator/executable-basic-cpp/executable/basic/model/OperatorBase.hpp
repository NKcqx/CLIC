/*****************************************************************************
*  所有Opt的基类，提供基础的属性和链接操作
*
*  @author   xxxxxxxm
*  @version  1.0
*
*****************************************************************************/
#ifndef OPERATOR_BASE_HPP
#define OPERATOR_BASE_HPP

#include <string>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include "Connection.hpp"
#include "ExecutionOperator.hpp"

namespace clic {
    using std::string;
    using std::vector;
    using std::map;
    using std::pair;

    class Connection;

    class OperatorBase : public ExecutionOperator {
        protected:
            string name;
            string ID;
            map<string, void*> inputData;           // 输入数据
            map<string, void*> outputData;          // 输出数据
            map<string, string> params;             // （输入）参数
            vector<Connection*> inputConnections;   // 所有的上一跳
            vector<Connection*> outputConnections;  // 所有的下一跳
            int inDegree;                           // 入度，拓扑排序使用

        public:
            OperatorBase(string _name, string id, vector<string> &inputKeys, vector<string> &outputKeys, map<string, string> _params)
                : name(_name), ID(id), params(_params) {
                this -> inDegree = 0;
                for(string inputKey : inputKeys) {
                    inputData.insert(make_pair(inputKey, nullptr));
                }

                for(string outputKey : outputKeys) {
                    outputData.insert(make_pair(outputKey, nullptr));
                }
            }

            virtual ~OperatorBase(){};

            string getName() {
                return this -> name;
            }

            void setParams(string key, string value) {
                this -> params.insert(make_pair(key, value));
            }

            void setInputData(string key, void *data) {
                // this -> inputData.insert(make_pair(key, data));
                this -> inputData[key] = data;
            }

            void* getInputData(string key) {
                return this -> inputData.count(key) > 0 ? this -> inputData[key] : nullptr;
            }

            void setOutputData(string key, void *data) {
                // this -> outputData.insert(make_pair(key, data));
                this->outputData[key] = data;
            }

            void* getOutputData(string key) {
                return this -> outputData.count(key) > 0 ? this -> outputData[key] : nullptr;
            }

            /**
             * 更新节点的入度
             *
             * @param delta 入度的变化值
             * @return 当前节点的入度值
             */
            int updateInDegree(int delta) {
                this -> inDegree += delta;
                return this -> inDegree;
            }

            int getInDegree() {
                return this -> inDegree;
            }

            vector<Connection*> getInputConnection() {
                return this -> inputConnections;
            }

            vector<Connection*> getOutputConnection() {
                return this -> outputConnections;
            }

            void connectTo(string sourceKey, OperatorBase* targetOpt, string targetKey) {
                for(Connection *connection : outputConnections) {
                    if(connection -> getTargetOpt() == targetOpt) {
                        connection -> addKey(sourceKey, targetKey);
                        return ;
                    }
                }
                Connection *tmpConnection = new Connection(this, sourceKey, targetOpt, targetKey);
                outputConnections.push_back(tmpConnection);
            }

            void connectTo(Connection *connection) {
                if(connection -> getSourceOpt() == this || connection -> getTargetOpt() == this) {
                    outputConnections.push_back(connection);
                } else {
                    throw "Connection 的两端未包含当前Opt, Connection："
                        + connection -> getSourceOpt() -> getName()
                        + " -> "
                        + connection -> getTargetOpt() -> getName()
                        + "。当前Operator"
                        + this -> name;
                }
            }

            void disConnectTo(Connection *connection) {
                vector<Connection*>::iterator iter = find(outputConnections.begin(), outputConnections.end(), connection); 
                if(iter != outputConnections.end()) {   // 如果包含则删除
                    outputConnections.erase(iter);
                }
            }

            void connectFrom(string targetKey, OperatorBase* sourceOpt, string sourceKey) {
                for(Connection *connection : outputConnections) {
                    if(connection -> getSourceOpt() == sourceOpt) {
                        connection -> addKey(sourceKey, targetKey);
                        return ;
                    }
                }
                Connection *tmpConnection = new Connection(sourceOpt, sourceKey, this, targetKey);
                outputConnections.push_back(tmpConnection);
                this -> updateInDegree(1);              // 更新当前opt的入度
            }

            void connectFrom(Connection *connection) {
                if(connection -> getSourceOpt() == this || connection -> getTargetOpt() == this) {
                    outputConnections.push_back(connection);
                    this -> updateInDegree(1);          // 更新当前opt的入度
                } else {
                    throw "Connection 的两端未包含当前Opt, Connection："
                        + connection -> getSourceOpt() -> getName()
                        + " -> "
                        + connection -> getTargetOpt() -> getName()
                        + "。当前Operator"
                        + this -> name;
                }
            }

            void disConnectFrom(Connection *connection) {
                vector<Connection*>::iterator iter = find(outputConnections.begin(), outputConnections.end(), connection); 
                if(iter != outputConnections.end()) {   // 如果包含则删除
                    outputConnections.erase(iter);
                    this -> updateInDegree(-1);         // 更新当前opt的入度
                }
            }
    };
}
#endif