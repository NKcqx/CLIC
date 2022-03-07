/*****************************************************************************
*  实现对于任务的通知机制
*
*  @author   xxm
*  @version  1.0
*
*****************************************************************************/
#ifndef NOTIFY_SERVICE_CLIENT_HPP
#define NOTIFY_SERVICE_CLIENT_HPP

#include <string>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TSocket.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "../../thriftGen/notifyservice/NotifyService.h"


namespace clic {
    using std::string;
    using ::apache::thrift::protocol::TBinaryProtocol;
    using ::apache::thrift::protocol::TProtocol;
    using ::apache::thrift::transport::TSocket;
    using ::apache::thrift::transport::TFramedTransport;
    using ::apache::thrift::transport::TTransport;
    

    class Client {
        private:
            NotifyServiceClient *client; // 调用的client
            std::shared_ptr<TSocket> socket;
            std::shared_ptr<TTransport> transport;
            std::shared_ptr<TProtocol> protocol;
            int stageId;        // 当前client的ID标识
            string jobName;     // 当前的jobName
            bool isDebug;       // 标记是否是debug模式，不尝试和master连接

        public:
            Client(int _stageId, string _jobName, string _host, int _port) {
                // 如果参数为空，推断出是本地模式，不会和master建立thrift连接
                if(_host.empty() || _port == 0) {
                    this -> isDebug = true;
                } else {
                    this -> isDebug = false;
                    
                    this -> socket = std::make_shared<TSocket>(_host, _port);
                    this -> transport = std::make_shared<TFramedTransport>(socket);
                    this -> protocol = std::make_shared<TBinaryProtocol>(transport);
                    this -> client = new NotifyServiceClient(this -> protocol);

                    this -> stageId = _stageId;
                    this -> jobName = _jobName;
                }
            }

            ~Client() {
                if(!this -> isDebug) {
                    this -> transport -> close();
                }
                delete this -> client;
            }

            // 向上游上传信息
            void notify(StageSnapshot snapshot) {
                if(this -> isDebug) {
                    // log info
                    std::cout << "Debug info: " << snapshot.message << std::endl;
                    return;
                }
                try {
                    this -> transport -> open();
                    this -> client -> postStatus(this -> jobName, this -> stageId, snapshot);
                } catch(const char* msg) {
                    std::cerr << msg << std::endl;
                }
            }
    };
}

#endif