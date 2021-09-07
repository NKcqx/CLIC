package fdu.daslab.executable.hpc.operators;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author zjchen
 * @time 2021/9/6 10:25 上午
 * @description username、password、remote_host、remote_port(optional)、command
 */

public class MpiRun extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {
    Logger logger = LoggerFactory.getLogger(MpiRun.class);

    private static final int SESSION_TIMEOUT = 10000;
    private static final int CHANNEL_TIMEOUT = 5000;

    public MpiRun(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("MpiRun", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        Session jschSession = null;
        try {
            JSch jsch = new JSch();
            jschSession = jsch.getSession(this.params.get("username"),
                    this.params.get("remote_host"),
                    Integer.parseInt(this.params.getOrDefault("remote_port", "22")));
            jschSession.setConfig("StrictHostKeyChecking", "no");
            jschSession.setPassword(this.params.get("password"));

            // 10 seconds timeout session
            jschSession.connect(SESSION_TIMEOUT);

            ChannelExec channelExec = (ChannelExec) jschSession.openChannel("exec");

            // run a shell script
            channelExec.setCommand(this.params.get("command"));

            // display errors to System.err
            channelExec.setErrStream(System.err);

            InputStream in = channelExec.getInputStream();

            // 5 seconds timeout channel
            channelExec.connect(CHANNEL_TIMEOUT);
            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    System.out.print(new String(tmp, 0, i));
                }
                if (channelExec.isClosed()) {
                    if (in.available() > 0) continue;
                    System.out.println("exit-status: "
                            + channelExec.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }

            channelExec.disconnect();

        } catch (JSchException | IOException e) {

            e.printStackTrace();

        } finally {
            if (jschSession != null) {
                jschSession.disconnect();
            }
        }
    }
}
