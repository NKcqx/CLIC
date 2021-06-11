package fdu.daslab.executorcenter.executor;

import fdu.daslab.executorcenter.adapter.ParamAdapter;
import fdu.daslab.executorcenter.local.LocalJars;
import fdu.daslab.thrift.base.Stage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.*;
import java.util.List;

/**
 * 本地执行器，为了方便策略
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/23/21 5:37 PM
 */
@Component
public class LocalExecutor implements Executor {

    private Logger logger = LoggerFactory.getLogger(LocalExecutor.class);

    @Resource
    private LocalJars localJars;

    @Autowired
    private ParamAdapter paramAdapter;

    @Override
    public void execute(Stage stage) {
        // 直接将参数传入，调用指定的jar包
        String jar = localJars.getJarForPlatform(stage.platformName);
        List<String> params = paramAdapter.wrapperExecutionArguments(stage);
        try {
            final Process process = Runtime.getRuntime().exec("java -jar " + jar + " " +
                    StringUtils.joinWith(" ", params.toArray()));
            SequenceInputStream sis = new SequenceInputStream(process.getInputStream(), process.getErrorStream());
            BufferedReader br =  new BufferedReader( new InputStreamReader(sis));
            String line;
            while ((line = br.readLine()) !=  null) {
                logger.info(line);
            }
            process.waitFor();
            process.destroy();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
