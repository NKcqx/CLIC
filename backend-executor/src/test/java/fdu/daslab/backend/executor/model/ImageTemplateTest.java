package fdu.daslab.backend.executor.model;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Testing for ImageTemplate.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:42
 */
public class ImageTemplateTest {

    private ImageTemplate javaTemplate;
    private ImageTemplate sparkTemplate;
    private ImageTemplate alchemistTemplate;

    @Before
    public void setUp() {
        javaTemplate = new ImageTemplate();
        javaTemplate.setPlatform("java");
        javaTemplate.setImage("executable-java:v1");
        javaTemplate.setCommand(Arrays.asList(("/bin/sh -c").split(" ")));
        // 把executor和所有的arg按照空格拼装在一起构成运行的命令
        String javaExecutor = "java -jar";
        String javaArgs = "executable-java.jar --udfPath= ";
        javaTemplate.setParamPrefix(javaExecutor + " " + javaArgs);

        sparkTemplate = new ImageTemplate();
        sparkTemplate.setPlatform("spark");
        sparkTemplate.setImage("executable-spark:v0");
        sparkTemplate.setCommand(Arrays.asList(("/bin/sh -c").split(" ")));
        // 把executor和所有的arg按照空格拼装在一起构成运行的命令
        String sparkExecutor = "/spark-runner/bin/spark-submit";
        String sparkArgs = "--master=k8s://https://10.141.221.219:6443 "
                + "--deploy-mode=client --name=spark-running "
                + "--class=fdu.daslab.executable.spark.ExecuteSparkOperator "
                + "--conf=spark.executor.instance=5 "
                + "--conf=spark.kubernetes.container.image=spark:v0 executable-spark.jar --udfPath= ";
        sparkTemplate.setParamPrefix(sparkExecutor + " " + sparkArgs);

        alchemistTemplate = new ImageTemplate();
        alchemistTemplate.setPlatform("alchemist");
        alchemistTemplate.setImage("executable-alchemist:v1");
        alchemistTemplate.setCommand(Arrays.asList(("/bin/sh -c").split(" ")));
        // 把executor和所有的arg按照空格拼装在一起构成运行的命令
        String alchemistExecutor = "mpiexec";
        String alchemistArgs = "-n=1 alchemist --udfPath= ";
        alchemistTemplate.setParamPrefix(alchemistExecutor + " " + alchemistArgs);
    }

    @Test
    public void getPlatform() {
        assertEquals("java", javaTemplate.getPlatform());
        assertEquals("spark", sparkTemplate.getPlatform());
        assertEquals("alchemist", alchemistTemplate.getPlatform());
    }

    @Test
    public void getImage() {
        assertEquals("executable-java:v1", javaTemplate.getImage());
        assertEquals("executable-spark:v0", sparkTemplate.getImage());
        assertEquals("executable-alchemist:v1", alchemistTemplate.getImage());
    }

    @Test
    public void getCommand() {
        assertEquals(Arrays.asList(("/bin/sh -c").split(" ")), javaTemplate.getCommand());
        assertEquals(Arrays.asList(("/bin/sh -c").split(" ")), sparkTemplate.getCommand());
        assertEquals(Arrays.asList(("/bin/sh -c").split(" ")), alchemistTemplate.getCommand());
    }

    @Test
    public void getParamPrefix() {
        assertEquals("java -jar executable-java.jar --udfPath= ", javaTemplate.getParamPrefix());
        assertEquals("mpiexec -n=1 alchemist --udfPath= ", alchemistTemplate.getParamPrefix());
    }
}
