#ifndef OPERATOR_FACTORY_HPP
#define OPERATOR_FACTORY_HPP

#include <vector>
#include <string>
#include <map>
#include <set>
#include "OperatorBase.hpp"
// #include "../utils/IocContainer.hpp"

namespace clic {
    using std::string;
    using std::vector;
    using std::map;
    using std::pair;
    using std::set;
    
    class OperatorFactory {
        protected:
            // map<string, datatype> operatorMap;
            set<string> operatorSet;
            // IocContainer ioc;

            /**
             * 判断集合中是否包含指定算子
             **/
            bool isIncluded(string name) {
                return this -> operatorSet.find(name) != this -> operatorSet.end();
            }

        public:
            // template <class T>
            // void addToIocContainer(string name) {
            //     ioc.RegisterType<OperatorBase, T, string, vector<string>&, vector<string>&, map<string, string>&>(name);
            // }
            // OperatorBase* createOperator(string name, string ID, vector<string> &inputKeys, vector<string> &outputKeys, map<string, string> &params) {
            //     return this -> ioc.ResolveShared<OperatorBase>(name, ID, inputKeys, outputKeys, params).get();
            // }
            virtual OperatorBase* createOperator(string name, string ID, vector<string> &inputKeys, vector<string> &outputKeys, map<string, string> params) = 0;
            virtual ~OperatorFactory(){};  
    };
}

#endif