package fdu.daslab.executorcenter.executor;

import fdu.daslab.thrift.base.Stage;
import org.apache.thrift.transport.TTransportException;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/23/21 5:39 PM
 */
public interface Executor {

    void execute(Stage stage) throws TTransportException;

//    Stage findStageStatus(Stage stage) throws TTransportException;
}
