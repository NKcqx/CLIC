package api;

import driver.CLICScheduler;

import java.util.List;

/**
 * 为了支持多任务，添加一个Context，用于任务的提交 和 资源管理  、 调度相关
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/23 10:14 AM
 */
public class ClicContext {

    // 调度器，一直会在后台轮询，直到结束
    private CLICScheduler clicScheduler = new CLICScheduler();

    // master的ip和port TODO: 本地的模式
    private String masterIP;
    private Integer masterPort;

    public ClicContext(String masterIP, Integer masterPort) {
        this.masterIP = masterIP;
        this.masterPort = masterPort;
    }

    // 保存所有用户的PlanBuilder
    private List<PlanBuilder> planBuilderList;
}
