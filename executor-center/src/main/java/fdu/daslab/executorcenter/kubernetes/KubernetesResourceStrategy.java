package fdu.daslab.executorcenter.kubernetes;

import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.base.Stage;

import java.util.List;

/**
 * 定义一个k8s资源的策略模式，其中包含创建新的资源，以及查看资源的状态
 *
 * @author 唐志伟
 * @version 1.0
 * @since 6/6/21 2:58 PM
 */
public interface KubernetesResourceStrategy {

    /**
     * 创建新的资源
     *
     * @param stage        任务信息
     * @param platformInfo 平台相关配置信息
     * @param params       动态的参数信息
     */
    void create(Stage stage, Platform platformInfo, List<String> params);
}
