package fdu.daslab.optimizercenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.optimizercenter.OptimizerModel;
import fdu.daslab.thrift.optimizercenter.OptimizerPlugin;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/19/21 1:23 PM
 */
public class OptimizerPluginClient extends ThriftClient<OptimizerPlugin.Client> {

    private OptimizerModel optimizerModel;

    public OptimizerPluginClient(OptimizerModel optimizerModel) {
        this.optimizerModel = optimizerModel;
    }

    @Override
    protected OptimizerPlugin.Client createClient(TBinaryProtocol protocol) {
        return new OptimizerPlugin.Client(protocol);
    }

    @Override
    protected String getHost() {
        return optimizerModel.getHost();
    }

    @Override
    protected int getPort() {
        return optimizerModel.getPort();
    }
}
