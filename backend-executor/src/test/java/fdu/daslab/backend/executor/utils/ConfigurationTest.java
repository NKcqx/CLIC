package fdu.daslab.backend.executor.utils;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.junit.Assert.*;

/**
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:42
 */
public class ConfigurationTest {

    private Configuration configuration;

    @Before
    public void setUp() throws Exception {

    }

//    @Test(expected = FileNotFoundException.class)
//    public void getProperty() throws FileNotFoundException {
//        // 这里抛出FileNotFoundException异常
//        // 是否是Configuration类有bug
//        // 还是我在这的构造函数用法不对？
//        configuration = new Configuration();
//
//        String resPath1 = configuration.getProperty("yaml-output-path");
//        String resPath2 = configuration.getProperty("yaml-prefix");
//
//        assertEquals("/tmp/irdemo_output/", resPath1);
//        assertEquals("job-", resPath2);
//    }
}
