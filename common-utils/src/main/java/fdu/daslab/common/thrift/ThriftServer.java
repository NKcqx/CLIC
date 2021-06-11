package fdu.daslab.common.thrift;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * thrift server的启动类
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 10:28 PM
 */
public class ThriftServer {
    private static final Logger logger = LoggerFactory.getLogger(ThriftServer.class);

    // 启动单个handler
    public static void start(int port, TProcessor processor) {
        try {
            TServerTransport transport = new TServerSocket(port);
            TThreadPoolServer.Args tArgs = new TThreadPoolServer.Args(transport);
            tArgs.processor(processor);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            TTransportFactory transportFactory = new TTransportFactory();
            tArgs.protocolFactory(protocolFactory);
            tArgs.transportFactory(transportFactory);
            TServer server = new TThreadPoolServer(tArgs);
            logger.info("thrift服务启动成功, 端口={}", port);
            server.serve();
        } catch (Exception e) {
            logger.error("thrift服务启动失败", e);
        }
    }

    // 启动多个handler，这个方法被废弃，不允许使用，因为上下游调用负担变重
    @Deprecated
    public static void start(int port, TProcessor ...processors) {
        try {
            TServerTransport transport = new TServerSocket(port);

            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            TMultiplexedProcessor multiplexedProcessor = new TMultiplexedProcessor();
            for (TProcessor processor : processors) {
                String serviceName = processor.toString();
                multiplexedProcessor.registerProcessor(serviceName, processor);
                logger.info("注册服务{}", serviceName);
            }
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(transport);
            serverArgs.processor(multiplexedProcessor);
            serverArgs.protocolFactory(protocolFactory);
            TServer server = new TThreadPoolServer(serverArgs);
            logger.info("thrift服务启动成功, 端口={}", port);
            server.serve();
        } catch (Exception e) {
            logger.error("thrift服务启动失败", e);
        }
    }
}
