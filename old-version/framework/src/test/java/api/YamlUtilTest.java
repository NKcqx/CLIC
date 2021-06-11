package api;

import adapters.ArgoAdapter;
import basic.Configuration;
import basic.operators.Operator;
import basic.operators.OperatorFactory;
import basic.platforms.PlatformFactory;
import fdu.daslab.backend.executor.model.ArgoNode;
import fdu.daslab.backend.executor.model.ImageTemplate;
import fdu.daslab.backend.executor.utils.TemplateUtil;
import fdu.daslab.backend.executor.utils.YamlUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Testing for YamlUtil.java
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/7/14 16:41
 */
public class YamlUtilTest {

    private String argoDag;
    private YamlUtil spyYamlUtil;
    private Map<String, Object> argoDagMap;

    private List<ArgoNode> tasks;
    private List<ImageTemplate> imageTemplateList;

    @Before
    public void setUp() throws Exception {
        argoDag = Objects.requireNonNull(YamlUtil.class.getClassLoader()
                .getResource("templates/argo-dag-simple.yaml")).getPath();
        spyYamlUtil = spy(new YamlUtil());

        basic.Configuration configuration = new Configuration();
        OperatorFactory.initMapping(configuration.getProperty("operator-mapping-file"));
        PlatformFactory.initMapping(configuration.getProperty("platform-mapping-file"));

        Operator opt1 = OperatorFactory.createOperator("source");
        Operator opt2 = OperatorFactory.createOperator("filter");
        Operator opt3 = OperatorFactory.createOperator("map");
        Operator opt4 = OperatorFactory.createOperator("reduce-by-key");
        Operator opt5 = OperatorFactory.createOperator("sort");
        Operator opt6 = OperatorFactory.createOperator("sink");

        opt1.selectEntity("java");
        opt2.selectEntity("java");
        opt3.selectEntity("spark");
        opt4.selectEntity("java");
        opt5.selectEntity("java");
        opt6.selectEntity("java");

        List<Operator> allOperators = new ArrayList<>();
        allOperators.add(opt1);
        allOperators.add(opt2);
        allOperators.add(opt3);
        allOperators.add(opt4);
        allOperators.add(opt5);
        allOperators.add(opt6);

        ArgoAdapter argoAdapter = new ArgoAdapter();
        //tasks = argoAdapter.adaptOperator(allOperators);
        imageTemplateList = argoAdapter.generateTemplateByConfig();
    }

    @Test
    public void createArgoYaml() {
//        String resultPath = spyYamlUtil.createArgoYaml(tasks, imageTemplateList);
//
//        assertEquals("/tmp/irdemo_output/job-xxx.yaml", resultPath.substring(0, 22) + "-xxx.yaml");
    }

    @Test
    public void joinYaml() {
//        argoDagMap = spyYamlUtil.readYaml(argoDag);
//
//        String resultPath = spyYamlUtil.joinYaml(tasks, imageTemplateList);
//
//        assertEquals("/tmp/irdemo_output/job-xxx.yaml", resultPath.substring(0, 22) + "-xxx.yaml");
    }

    @Test
    public void joinTask() {
//        List<Object> newTasks = new LinkedList<>();
//        for (ArgoNode node : tasks) {
//            ImageTemplate imageTemplate = imageTemplateList.stream()
//                .filter(template -> template.getPlatform().equals(node.getPlatform()))
//                .collect(Collectors.toList())
//                .get(0);
//            newTasks.add(spyYamlUtil.joinTask(node, imageTemplate));
//            verify(spyYamlUtil, times(1)).joinTask(node, imageTemplate);
//        }
    }

    @Test
    public void readYaml() {
//        argoDagMap = spyYamlUtil.readYaml(argoDag);
//
//        verify(spyYamlUtil, times(1)).readYaml(argoDag);
    }

    @Test
    public void writeYaml() {
//        argoDagMap = spyYamlUtil.readYaml(argoDag);
//        String templatePath = TemplateUtil.getTemplatePathByPlatform("java");
//        spyYamlUtil.writeYaml(templatePath, argoDagMap);
//
//        verify(spyYamlUtil, times(1)).writeYaml(templatePath, argoDagMap);
    }
}
