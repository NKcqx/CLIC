package fdu.daslab;

import fdu.daslab.scheduler.CLICScheduler;
import fdu.daslab.scheduler.TaskScheduler;
import fdu.daslab.service.impl.SchedulerServiceImpl;
import fdu.daslab.service.impl.TaskServiceImpl;
import fdu.daslab.thrift.master.SchedulerService;
import fdu.daslab.thrift.master.TaskService;
import fdu.daslab.util.Configuration;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;

/**
 * clic的master，最终被暴露一个服务给外部调用，然后任务提交时需要指定master的地址和master连接，将整个plan提交给master
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/23 10:18 AM
 */
public class ClicMaster {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClicMaster.class);
    private static Integer masterThriftPort;

    static {
        try {
            Configuration configuration = new Configuration();
            masterThriftPort = Integer.valueOf(configuration.getProperty("clic-master-port"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /*
        启动两个service：TaskService 和 SchedulerService
            一个TaskService面向用户，用户提交任务和查看任务状态；
            一个SchedulerService面向kubernetes的pod，用于和pod进行交互
        同时会启动两个EventLoop，负责实际的处理逻辑: TaskScheduler 和 CLICScheduler
            一个TaskScheduler负责Task粒度，将任务的dag适配到kubernetes上，并切分为Stage将任务交给CLICScheduler；
            一个CLICScheduler负责Stage粒度，实际按照某种调度规则根据stage创建job并执行
     */

    public static void main(String[] args) throws TTransportException {
        // 启动clic的事件循环
        CLICScheduler clicScheduler = new CLICScheduler();
        clicScheduler.start();
        // 启动task的事件循环
        TaskScheduler taskScheduler = new TaskScheduler(clicScheduler);
        taskScheduler.start();

        // 启动thrift service
        LOGGER.info("Master thrift server start...");
        TMultiplexedProcessor tprocessor = new TMultiplexedProcessor();
        tprocessor.registerProcessor("SchedulerService", new SchedulerService.Processor<>(
                new SchedulerServiceImpl(clicScheduler)));
        tprocessor.registerProcessor("TaskService", new TaskService.Processor<>(
                new TaskServiceImpl(taskScheduler)));
        TServerSocket tServerSocket = new TServerSocket(masterThriftPort);
        TThreadPoolServer.Args ttpsArgs = new TThreadPoolServer.Args(tServerSocket);
        ttpsArgs.processor(tprocessor);
        ttpsArgs.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TThreadPoolServer(ttpsArgs);
        server.serve();
        LOGGER.info("Master thrift server stop!");
    }
}
