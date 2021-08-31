#ifndef DAG_EXECUTOR_HPP
#define DAG_EXECUTOR_HPP

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "DagHook.hpp"
#include "DagArgs.hpp"
#include "../service/client/NotifyServiceClient.hpp"
#include "basic/model/OperatorBase.hpp"
#include "basic/utils/TopoTraversal.hpp"

namespace clic {
    using std::string;
    using std::map;
    using std::pair;
    using std::vector;

    class DagExecutor{
        private:
            // 平台的独有参数
            map<string, string> platformArgs;

            // 平台共有参数
            DagArgs* basicArgs;

            // 一些平台特殊的处理逻辑
            DagHook* hook;

            // 当前dag的operator的头节点
            vector<OperatorBase*> headOperators;

            // 用于通知的client
            Client* notifyServiceClient;

            void initArgs(int argc, char* argv[]) {
                basicArgs = new DagArgs(argc, argv);
            }

            void initNotifyClient() {
                // 创建一个thrift client，用于和master进行交互
                this -> notifyServiceClient = new Client(this -> basicArgs -> stageId,
                                                        this -> basicArgs -> jobName,
                                                        this -> basicArgs -> notifyHost,
                                                        this -> basicArgs -> notifyPort);
            }

            void initOperators(ArgUtil &parser) {
                try {
                    parser.parse(this -> basicArgs -> dagPath, this -> headOperators);
                } catch (const char* msg) {
                    std::cerr << msg << std::endl;
                    string _message = msg;
                    StageSnapshot _shot;
                    _shot.__set_status(StageStatus::type::FAILURE);
                    _shot.__set_message(_message);
                    notifyServiceClient -> notify(_shot);
                    exit(-1);
                }
            }

            void executeDag() {
                TopoTraversal topoTraversal = TopoTraversal(this -> headOperators);
                while(topoTraversal.hasNextOpt()) {
                    OperatorBase* curOpt = topoTraversal.nextOpt();
                    std::cout << "Current operator: " << curOpt -> getName() << std::endl;
                    curOpt -> execute();

                    // 将计算结果传递到每个下一跳的 operator
                    vector<Connection*> connections = curOpt -> getOutputConnection();
                    
                    for(Connection* connection : connections) {
                        OperatorBase* targetOpt = connection -> getTargetOpt();
                        topoTraversal.updateIndegree(targetOpt, -1);
                        vector<pair<string, string>> keyPairs = connection -> getKeys();
                        for(pair<string, string> keyPair : keyPairs) {
                            if(!(keyPair.first.empty() || keyPair.second.empty())) {
                                void *sourceResult = curOpt -> getOutputData(keyPair.first);
                                // 将当前 operator 的输出结果传入下一跳，作为其输入数据
                                targetOpt -> setInputData(keyPair.second, sourceResult);
                            }
                        }
                    }
                }
            }
        
        public:
            DagExecutor(int argc, char* argv[], ArgUtil &parser) {
                // 解析参数，分别获取basicArgs和platformArgs
                this -> initArgs(argc, argv);

                // 初始化master的客户端
                this -> initNotifyClient();

                // 读取dag文件，解析生成所有的operator列表
                this -> initOperators(parser);

                this -> hook = new DagHook();
            }

            DagExecutor(int argc, char* argv[], ArgUtil &parser, DagHook* _hook) {
                // 解析参数，分别获取basicArgs和platformArgs
                initArgs(argc, argv);

                // 初始化master的客户端
                initNotifyClient();

                // 读取dag文件，解析生成所有的operator列表
                initOperators(parser);
                this -> hook = _hook;
            }

            DagExecutor(vector<OperatorBase*> &heads) {
                this -> headOperators = heads;
            }

            ~DagExecutor() {
                delete this -> basicArgs;
                delete this -> hook;
                delete this -> notifyServiceClient;
            }

            void execute() {
                try {
                    // 前处理方法
                    this -> hook -> preHandler(platformArgs);
                    StageSnapshot shot;
                    shot.__set_status(StageStatus::type::RUNNING);
                    this -> notifyServiceClient -> notify(shot);

                    // 执行dag图
                    this -> executeDag();
                    shot.__set_status(StageStatus::type::COMPLETED);
                    this -> notifyServiceClient -> notify(shot);

                    // 后处理方法
                    this -> hook -> postHandler(platformArgs);
                } catch(const char* msg) {
                    std::cerr << msg << std::endl;
                    string _message = msg;
                    StageSnapshot _shot;
                    _shot.__set_status(StageStatus::type::FAILURE);
                    _shot.__set_message(_message);
                    notifyServiceClient -> notify(_shot);
                }
            }
    };
}

#endif
