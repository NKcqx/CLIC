package fdu.daslab.executable;

import java.util.Map;

/**
 * 实现一些hook方法，可以供每个平台去实现一些特殊的处理逻辑
 *
 * @author 唐志伟
 * @version 1.0
 * @since 12/23/20 4:52 PM
 */
public class DagHook {

    // stage执行之前的处理方法
    public void preHandler(Map<String, String> platformArgs) {

    }

    // stage执行之后的处理方法
    public void postHandler(Map<String, String> platformArgs) {

    }
}
