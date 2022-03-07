/*****************************************************************************
*  连接两个operator，实现operator之间的key的映射
*
*  @author   xxm
*  @version  1.0
*
*****************************************************************************/
#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <string>
#include <vector>
#include <utility>
#include "./OperatorBase.hpp"

namespace clic {
    using std::string;
    using std::vector;
    using std::map;
    using std::pair;

    class OperatorBase;
    
    class Connection {
        protected:
            OperatorBase *sourceOpt;
            vector<string> sourceKeys;
            OperatorBase *targetOpt;
            vector<string> targetKeys;
        
        public:
            Connection(OperatorBase* _sourceOpt, string _sourceKey, OperatorBase* _targetOpt, string _targetKey) : sourceOpt(_sourceOpt), targetOpt(_targetOpt) {
                this -> sourceKeys.push_back(_sourceKey);
                this -> targetKeys.push_back(_targetKey);
            }

            ~Connection() { }
            
            void addKey(string sourceKey, string targetKey) {
                this -> sourceKeys.push_back(sourceKey);
                this -> targetKeys.push_back(targetKey);
            }

            vector<pair<string, string>> getKeys() {
                vector<pair<string, string>> res;
                res.resize(sourceKeys.size());
                for(int i = 0; i < sourceKeys.size(); i++) {
                    res[i] = pair<string, string>(sourceKeys[i], targetKeys[i]);
                }
                return res;
            }

            OperatorBase* getSourceOpt() {
                return this -> sourceOpt;
            }

            vector<string> getSourceKeys() {
                return this -> sourceKeys;
            }

            OperatorBase* getTargetOpt() {
                return this -> targetOpt;
            }

            vector<string> getTargetKeys() {
                return this -> targetKeys;
            }

    };
}

#endif