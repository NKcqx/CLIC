/*****************************************************************************
*  对 Workflow 的 DAG 图执行拓扑排序
*
*  @author   xxm
*  @version  1.0
*
*****************************************************************************/
#ifndef TOPO_TRAVERSAL_HPP
#define TOPO_TRAVERSAL_HPP

#include <iostream>
#include <vector>
#include <queue>
#include "../model/OperatorBase.hpp"

namespace clic {
    using std::vector;
    using std::queue;

    class TopoTraversal {
        private:
            queue<OperatorBase*> que;
        
        public:
            /**
             * 提供 DAG 图中所有的头结点指针信息，作为构造函数的参数
             **/
            TopoTraversal(vector<OperatorBase*> &heads) {
                for(OperatorBase *opt : heads) {
                    this -> que.push(opt);
                }
            }

            ~TopoTraversal() { }

            bool hasNextOpt() {
                return !this -> que.empty();
            }
 
            OperatorBase* nextOpt() {
                if(this -> hasNextOpt()) {
                    OperatorBase *next = this -> que.front();
                    que.pop();
                    return next;
                }
                return nullptr;
            }

            void updateIndegree(OperatorBase *opt, int delta) {
                opt -> updateInDegree(delta);
                if(opt -> getInDegree() == 0) {
                    que.push(opt);
                }
            }
    };
}

#endif