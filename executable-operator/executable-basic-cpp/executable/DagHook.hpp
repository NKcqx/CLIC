#ifndef DAG_HOOK_HPP
#define DAG_HOOK_HPP

#include <map>
#include <string>

namespace clic {
    using std::map;
    using std::string;

    class DagHook {
        public:
            // stage执行之前的处理方法
            void preHandler(map<string, string> platformArgs){

            }

            // stage执行之后的处理方法
            void postHandler(map<string, string> platformArgs){

            }
    };
}

#endif