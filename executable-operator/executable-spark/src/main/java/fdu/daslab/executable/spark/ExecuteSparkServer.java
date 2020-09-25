package fdu.daslab.executable.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.service.impl.ExecuteSparkServiceImpl;
import fdu.daslab.executable.service.ExecuteService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * spark平台的server的启动类，会启动一个ThriftServer对外提供服务
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/22 3:54 PM
 */
@Parameters(separators = "=")
public class ExecuteSparkServer {

    @Parameter(names = {"--stageId", "-sid"})
    Integer stageId;    // 标识这一个stage

    @Parameter(names = {"--udfPath", "-udf"})
    String udfPath;

    @Parameter(names = {"--dagPath", "-dag"})
    String dagPath;

    @Parameter(names = {"--port", "-p"})
    Integer thriftPort; // 本server启动的thrift端口

    @Parameter(names = {"--driverHost", "-dh"})
    String driverHost; // driver的thrift地址

    @Parameter(names = {"--driverPort", "-dp"})
    Integer driverPort; // driver的thrift端口

    private static Logger logger = LoggerFactory.getLogger(ExecuteSparkServer.class);

    public static void main(String[] args) throws TTransportException {

        ExecuteSparkServer entry = new ExecuteSparkServer();
        JCommander.newBuilder()
                .addObject(entry)
                .build()
                .parse(args);

        TProcessor tprocessor = new ExecuteService.Processor<>(
                new ExecuteSparkServiceImpl(entry.stageId, entry.udfPath, entry.dagPath,
                        entry.driverHost, entry.driverPort));
        TServerSocket tServerSocket = new TServerSocket(entry.thriftPort);
        TThreadPoolServer.Args ttpsArgs = new TThreadPoolServer.Args(tServerSocket);
        ttpsArgs.processor(tprocessor);
        ttpsArgs.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TThreadPoolServer(ttpsArgs);

        //  启动server，接收请求
        logger.info("Spark thrift server start, at: " + tServerSocket.getServerSocket().toString());
        server.serve();
    }
}
