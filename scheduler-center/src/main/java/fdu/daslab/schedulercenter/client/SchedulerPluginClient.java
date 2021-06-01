package fdu.daslab.schedulercenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.schedulercenter.SchedulerModel;
import fdu.daslab.thrift.schedulercenter.SchedulerPlugin;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/24/21 4:45 PM
 */
public class SchedulerPluginClient extends ThriftClient<SchedulerPlugin.Client> {

    private SchedulerModel schedulerModel;

    public SchedulerPluginClient(SchedulerModel schedulerModel) {
        this.schedulerModel = schedulerModel;
    }

    @Override
    protected SchedulerPlugin.Client createClient(TBinaryProtocol protocol) {
        return new SchedulerPlugin.Client(protocol);
    }

    @Override
    protected String getHost() {
        return schedulerModel.host;
    }

    @Override
    protected int getPort() {
        return schedulerModel.port;
    }
}
