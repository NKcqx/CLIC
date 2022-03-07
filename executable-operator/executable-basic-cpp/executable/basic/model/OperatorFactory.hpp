/*****************************************************************************
*  所有CPP平台的OperatorFactory抽象基类，子类须实现createOperator方法
*
*  @author   xxm
*  @version  1.0
*
*****************************************************************************/
#ifndef OPERATOR_FACTORY_HPP
#define OPERATOR_FACTORY_HPP

#include <vector>
#include <string>
#include <map>
#include <set>
#include "OperatorBase.hpp"

namespace clic {
    using std::string;
    using std::vector;
    using std::map;
    using std::pair;
    using std::set;
    
    class OperatorFactory {
        protected:
            set<string> operatorSet;

            /**
             * 判断集合中是否包含指定算子
             **/
            bool isIncluded(string name) {
                return this -> operatorSet.find(name) != this -> operatorSet.end();
            }

        public:
            virtual OperatorBase* createOperator(string name, string ID, vector<string> &inputKeys, vector<string> &outputKeys, map<string, string> params) = 0;
            virtual ~OperatorFactory(){};  
    };
}

#endif