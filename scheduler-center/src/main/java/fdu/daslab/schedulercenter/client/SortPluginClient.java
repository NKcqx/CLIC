package fdu.daslab.schedulercenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.schedulercenter.SchedulerModel;
import fdu.daslab.thrift.schedulercenter.SortPlugin;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/24/21 4:45 PM
 */
public class SortPluginClient extends ThriftClient<SortPlugin.Client> {

    private SchedulerModel schedulerModel;

    public SortPluginClient(SchedulerModel schedulerModel) {
        this.schedulerModel = schedulerModel;
    }

    @Override
    protected SortPlugin.Client createClient(TBinaryProtocol protocol) {
        return new SortPlugin.Client(protocol);
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
