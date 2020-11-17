package fdu.daslab.redis;

import com.google.gson.Gson;
import fdu.daslab.scheduler.model.Task;
import fdu.daslab.util.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

import java.io.FileNotFoundException;
import java.util.*;

/**
 * 对于任务信息的增删改查
 *
 * @author 唐志伟
 * @version 1.0
 * @since 11/16/20 3:31 PM
 */
public class TaskRepository {

    // redis的task key前缀
    private static String taskKeyPrefix;
    // 默认的失效时间
    private static Integer expireSeconds;
    // Gson的设置
    private static Gson gson = new Gson();

    static {
        try {
            Configuration configuration = new Configuration();
            taskKeyPrefix = configuration.getProperty("task-key-prefix");
            expireSeconds = Integer.valueOf(configuration.getProperty("task-expire-time"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 获取所有task的key
    private Set<String> scanKeys() {
        Set<String> taskKeys = new HashSet<>();
        try (Jedis jedis = JedisUtil.getClient()) {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams();
            scanParams.match(taskKeyPrefix + "*");
            scanParams.count(1000); // 每次扫描的key的数量
            do {
                ScanResult<String> ret = jedis.scan(cursor, scanParams);
                List<String> result = ret.getResult();
                if (result != null) {
                    taskKeys.addAll(result);
                }
                // 再处理cursor
                cursor = ret.getCursor();
                // 迭代一直到cursor变为0为止
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        }
        return taskKeys;
    }

    /**
     * 查找所有的Task的状态，master加载时才会scan，因此影响不大
     *
     * @return taskName - task
     */
    public Map<String, Task> listAllTask() {
        Map<String, Task> taskMap = new HashMap<>();
        Set<String> taskKeys = scanKeys();
        try (Jedis jedis = JedisUtil.getClient()) {
            for (String taskKey : taskKeys) {
                String taskJson = jedis.get(taskKey);
                Task task = gson.fromJson(taskJson, Task.class);
                taskMap.put(task.getPlanName(), task);
            }
        }
        return taskMap;
    }

    /**
     * 插入/更新task，<b>注意设置一定的失效时间，防止占用内存太多</b>
     *
     * @param task task的信息
     */
    public void addOrUpdateTaskInfo(Task task) {
        try (Jedis jedis = JedisUtil.getClient()) {
            String taskJson = gson.toJson(task);
            SetParams setParams = new SetParams();
            setParams.ex(expireSeconds);
            jedis.set(taskKeyPrefix + task.getPlanName(), taskJson, setParams);
        }
    }

    /**
     * 查看某个task的信息
     *
     * @param planName 任务名称
     * @return task
     */
    public Task getTaskInfo(String planName) {
        Task task;
        try (Jedis jedis = JedisUtil.getClient()) {
            String taskKey = taskKeyPrefix + planName;
            String taskJson = jedis.get(taskKey);
            task = gson.fromJson(taskJson, Task.class);
        }
        return task;
    }

    /**
     * 根据taskName 删除某个task
     *
     * @param planName 任务名称
     */
    public void deleteTask(String planName) {
        // TODO: 考虑失效时候 / 删除时候的相关stage的级联删除
        try (Jedis jedis = JedisUtil.getClient()) {
            String taskKey = taskKeyPrefix + planName;
            jedis.del(taskKey);
        }
    }

}
