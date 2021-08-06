package fdu.daslab.redis;

import com.google.gson.Gson;
import fdu.daslab.scheduler.model.KubernetesStage;
import fdu.daslab.scheduler.model.StageStatus;
import fdu.daslab.util.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;

/**
 * 对于stage信息的增删改查
 *
 * @author 唐志伟
 * @version 1.0
 * @since 11/16/20 4:37 PM
 */
public class StageRepository {

    // redis的stage key前缀
    private static String stageKeyPrefix;
    // Gson的设置
    private static Gson gson = new Gson();

    static {
        try {
            Configuration configuration = new Configuration();
            stageKeyPrefix = configuration.getProperty("stage-key-prefix");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入 / 更新 指定stage的信息
     *
     * @param stage stage信息
     */
    public void addOrUpdateStage(KubernetesStage stage) {
        try (Jedis jedis = JedisUtil.getClient()) {
            String stageJson = gson.toJson(stage);
            jedis.set(stageKeyPrefix + stage.getStageId(), stageJson);
        }
    }

    /**
     * 批量插入stage信息
     *
     * @param stages stage信息
     */
    public void addStageBatch(Collection<KubernetesStage> stages) {
        try (Jedis jedis = JedisUtil.getClient()) {
            final Pipeline pipeline = jedis.pipelined();
            stages.forEach(stage -> {
                String stageJson = gson.toJson(stage);
                pipeline.set(stageKeyPrefix + stage.getStageId(), stageJson);
            });
            pipeline.sync();
            pipeline.close();
        }
    }

    /**
     * 选择性更新，只更新有值的字段，这个方法需要反射，性能不高，暂时不用
     *
     * @param stage 更新的stage信息
     */
    public void updateStageSelective(KubernetesStage stage) throws IntrospectionException,
            InvocationTargetException, IllegalAccessException {
        // 查询stage信息
        KubernetesStage oldStage = getStageInfo(stage.getStageId());
        if (oldStage != null) {
            // 只更新有值的字段
            Field[] fields = oldStage.getClass().getDeclaredFields();
            final Class<? extends KubernetesStage> clazz = oldStage.getClass();
            for (Field field : fields) {
                PropertyDescriptor pd = new PropertyDescriptor(field.getName(), clazz);
                // get方法
                Method getMethod = pd.getReadMethod();
                Object value = getMethod.invoke(stage);
                if (value != null) {
                    // set方法
                    Method setMethod = pd.getWriteMethod();
                    setMethod.invoke(oldStage, value);
                }
            }
            addOrUpdateStage(oldStage);
        }
    }

    /**
     * 提交某个stage开始运行
     *
     * @param stageId stage标识
     */
    public void updateStageStarted(String stageId) {
        // 查询stage信息
        KubernetesStage oldStage = getStageInfo(stageId);
        oldStage.setStartTime(new Date());
        oldStage.setStageStatus(StageStatus.RUNNING);
        // 修改
        addOrUpdateStage(oldStage);
    }

    /**
     * 提交某个stage结束运行
     *
     * @param stageId stage标识
     */
    public void updateStageCompleted(String stageId) {
        // 查询stage信息
        KubernetesStage oldStage = getStageInfo(stageId);
        oldStage.setCompleteTime(new Date());
        oldStage.setStageStatus(StageStatus.COMPLETED);
        // 修改
        addOrUpdateStage(oldStage);
    }

    /**
     * 只修改stage的状态
     *
     * @param stageId stageId
     * @param status  状态
     */
    public void updateStageStatus(String stageId, StageStatus status) {
        // 查询stage信息
        KubernetesStage oldStage = getStageInfo(stageId);
        oldStage.setStageStatus(status);
        // 修改
        addOrUpdateStage(oldStage);
    }

    /**
     * 查看指定stage的详细信息
     *
     * @param stageId stageId
     * @return stage信息
     */
    public KubernetesStage getStageInfo(String stageId) {
        KubernetesStage stage;
        try (Jedis jedis = JedisUtil.getClient()) {
            String stageKey = stageKeyPrefix + stageId;
            String stageJson = jedis.get(stageKey);
            stage = gson.fromJson(stageJson, KubernetesStage.class);
        }
        return stage;
    }

    /**
     * 删除指定stage
     *
     * @param stageId stage的id
     */
    public void deleteStage(String stageId) {
        try (Jedis jedis = JedisUtil.getClient()) {
            String stageKey = stageKeyPrefix + stageId;
            jedis.del(stageKey);
        }
    }
}
