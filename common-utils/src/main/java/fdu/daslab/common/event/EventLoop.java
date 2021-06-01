package fdu.daslab.common.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 事件循环，事件循环器需要继承这个抽象类
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/23 8:40 PM
 */
public abstract class EventLoop<E> {

    private static Logger logger = LoggerFactory.getLogger(EventLoop.class);

    private String name = "Event Loop";

    public EventLoop(String name) {
        this.name = name;
    }

    // 事件队列
    private final BlockingQueue<E> eventQueue = new LinkedBlockingQueue<>();
    // 标记事件是否结束
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    // 事件线程，执行事件循环
    private final Thread eventThread = new Thread(name) {

        @Override
        public void run() {
            try {
                // 循环
                while (!stopped.get()) {
                    // 从事件队列中依次取出执行
                    final E event = eventQueue.take();
                    // 处理event
                    try {
                        onReceive(event);
                    } catch (Exception e) {
                        // 某个事件的执行不能影响其他的事件
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                logger.error("event error: " + e.getMessage());
            }
        }
    };

    // 需要子类实现相应的逻辑
    protected void onReceive(E event) {
    }

    // 启动事件循环
    public void start() {
        if (stopped.get()) {
            throw new IllegalStateException(name + " has already been stopped");
        }
        // 后台执行事件循环
        eventThread.setDaemon(true);
        eventThread.start();
    }

    // 停止事件循环
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            eventThread.interrupt();
        }
    }

    // 提交事件
    public void post(E event) throws InterruptedException {
        eventQueue.put(event);
    }
}

