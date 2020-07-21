package fdu.daslab.backend.executor.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Testing for YamlUtil.java
 *
 * JoinYaml函数里的
 * ImageTemplate imageTemplate = imageTemplates.stream()
 *                     .filter(template -> template.getPlatform().equals(node.getPlatform()))
 *                     .collect(Collectors.toList())
 *                     .get(0);
 * 操作尚未测试成功
 * 推测可能是本人测试有问题或者代码逻辑有问题
 * 请求支援
 * 测试代码在PipelineTest.java中
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:41
 */
public class YamlUtilTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void createArgoYaml() {
    }

    @Test
    public void joinYaml() {
    }

    @Test
    public void joinTask() {
    }

    @Test
    public void readYaml() {
    }

    @Test
    public void writeYaml() {
    }
}
