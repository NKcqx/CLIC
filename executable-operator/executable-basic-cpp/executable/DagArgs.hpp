/*****************************************************************************
*  dag的参数，通过命令行传入
*
*  @author   xxm
*  @version  1.0
*
*****************************************************************************/
#ifndef DAG_ARGS_HPP
#define DAG_ARGS_HPP

#include <string>
#include <map>
#include <utility>
#include "basic/utils/Clipp.hpp"

namespace clic {
    using std::string;
    using std::map;
    using std::pair;

    class DagArgs {
        /**
            Attributes:
                1. stageId      : 唯一的stageId
                2. dagPath      : 需要创建dag的Yaml文件路径
                3. notifyHost   : master的地址，提供给thrift实现远程调用
                4. notifyPort   : master启动的端口，提供给thrift实现远程调用
                5. platformArgs : 不同平台可能需要的参数，提供给DagHook执行额外的操作
                6. jobName      : 任务名称
                7. udfPath      : UDF路径（可选，通过hasUdf表示是否使用该参数）
        **/
        private:
            // 去除string类型参数的首尾空格
            string& removeHeadTailSpace(string &str) {
                if (!str.empty()) {
                    str.erase(0,str.find_first_not_of(" "));
                    str.erase(str.find_last_not_of(" ") + 1);
                }
                return str;
            }

        public:
            int stageId;
            string jobName;
            bool hasUdf;
            string udfPath;
            string dagPath;
            string notifyHost;
            int notifyPort;
            string platformArgs;
            DagArgs(int argc, char* argv[]) {
                this -> hasUdf = false;
                auto cli = (clipp::required("--stageId") & clipp::value("stageId", stageId),
                        clipp::required("--jobName") & clipp::value("jobName", jobName),
                        clipp::option("--udfPath").set(hasUdf) & clipp::value("udfPath", udfPath),
                        clipp::required("--dagPath") & clipp::value("dagPath", dagPath),
                        clipp::required("--notifyHost") & clipp::value("notifyHost", notifyHost),
                        clipp::required("--notifyPort") & clipp::value("notifyPort", notifyPort),
                        clipp::option("--D") & clipp::value("platformArgs", platformArgs)
                    );
                
                if (clipp::parse(argc, const_cast<char **>(argv), cli)) {
                    // 从yaml读入的参数有时会包含多余的空格，会影响后面的操作，因此对于string类型的参数都先去除首尾空格
                    removeHeadTailSpace(this -> jobName);
                    removeHeadTailSpace(this -> udfPath);
                    removeHeadTailSpace(this -> dagPath);
                    removeHeadTailSpace(this -> notifyHost);
                    removeHeadTailSpace(this -> platformArgs);
                    std::cout << "stageId: " << stageId << ",\n"
                            << "jobName: " << jobName  << ",\n"
                            << "flag: " << hasUdf << ",\n"
                            << "udfPath: " << udfPath << ",\n"
                            << "dagPath: " << dagPath << ",\n"
                            << "notifyHost: " << notifyHost << ",\n"
                            << "notifyPort: " << notifyPort << ",\n"
                            << "platformArgs: " << platformArgs << std::endl;
                } else {
                    // 打印参数用法
                    std::cerr << clipp::make_man_page(cli, argv[0]) << std::endl;
                    exit(-1);
                }
            }
             
    };
}

#endif