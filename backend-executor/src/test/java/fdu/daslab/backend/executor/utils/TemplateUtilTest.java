package fdu.daslab.backend.executor.utils;

import fdu.daslab.backend.executor.model.ImageTemplate;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.*;

/**
 * Testing for TemplateUtil.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:41
 */
public class TemplateUtilTest {

    private String templateSuffix;
    private String templateDir;

    @Before
    public void setUp() {
        templateSuffix = "-template";
        templateDir = Objects.requireNonNull(TemplateUtil.class.getClassLoader()
                .getResource("templates/")).getPath();
    }

    @Test
    public void getTemplateNameByPlatform() {
        String javaTemplateName = TemplateUtil.getTemplateNameByPlatform("java");
        String sparkTemplateName = TemplateUtil.getTemplateNameByPlatform("spark");

        assertEquals("java" + templateSuffix, javaTemplateName);
        assertEquals("spark" + templateSuffix, sparkTemplateName);
    }

    @Test
    public void getTemplatePathByPlatform() {
        String javaTemplatePath = TemplateUtil.getTemplatePathByPlatform("java");
        String sparkTemplatePath = TemplateUtil.getTemplatePathByPlatform("spark");

        assertEquals(templateDir + TemplateUtil.getTemplateNameByPlatform("java") + ".yaml",
                javaTemplatePath);
        assertEquals(templateDir + TemplateUtil.getTemplateNameByPlatform("spark") + ".yaml",
                sparkTemplatePath);
    }

    @Test
    public void getOrCreateTemplate() {
        ImageTemplate imageTemplate = new ImageTemplate();
        imageTemplate.setPlatform("java");
        imageTemplate.setImage("executable-java:v1");
        imageTemplate.setCommand(Arrays.asList(("/bin/sh -c").split(" ")));
        // 把executor和所有的arg按照空格拼装在一起构成运行的命令
        String executor = "java -jar";
        String args = "executable-java.jar --udfPath= ";
        imageTemplate.setParamPrefix(executor + " " + args);

        String templateName = TemplateUtil.getOrCreateTemplate(imageTemplate);
        assertEquals("java-template", templateName);
    }
}
