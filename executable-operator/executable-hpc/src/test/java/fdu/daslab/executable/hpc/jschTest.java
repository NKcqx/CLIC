package fdu.daslab.executable.hpc;

import org.junit.Test;
import com.jcraft.jsch.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * @author zjchen
 * @time 2021/9/6 10:39 上午
 * @description
 */

public class jschTest {
    private static final String REMOTE_HOST = "10.106.10.71";
    private static final String USERNAME = "usr-dDyDTogd";
    private static final String PASSWORD = "Sdsc@0199";
    private static final int REMOTE_PORT = 20023;
    private static final int SESSION_TIMEOUT = 10000;
    private static final int CHANNEL_TIMEOUT = 5000;

//    @Test
//    public void sshTest(){
//        Session jschSession = null;
//        try {
//            JSch jsch = new JSch();
////            jsch.setKnownHosts("/Users/zjchen/.ssh/known_hosts");
//            jschSession = jsch.getSession(USERNAME, REMOTE_HOST, REMOTE_PORT);
//            jschSession.setConfig("StrictHostKeyChecking", "no");
//            // not recommend, uses jsch.setKnownHosts
//            //jschSession.setConfig("StrictHostKeyChecking", "no");
//
//            // authenticate using password
//            jschSession.setPassword(PASSWORD);
//
//            // 10 seconds timeout session
//            jschSession.connect(SESSION_TIMEOUT);
//
//            ChannelExec channelExec = (ChannelExec) jschSession.openChannel("exec");
//
//            // run a shell script
//            channelExec.setCommand("mkdir test_ssh");
//
//            // display errors to System.err
//            channelExec.setErrStream(System.err);
//
//            InputStream in = channelExec.getInputStream();
//
//            // 5 seconds timeout channel
//            channelExec.connect(CHANNEL_TIMEOUT);
//            // read the result from remote server
//            byte[] tmp = new byte[1024];
//            while (true) {
//                while (in.available() > 0) {
//                    int i = in.read(tmp, 0, 1024);
//                    if (i < 0) break;
//                    System.out.print(new String(tmp, 0, i));
//                }
//                if (channelExec.isClosed()) {
//                    if (in.available() > 0) continue;
//                    System.out.println("exit-status: "
//                            + channelExec.getExitStatus());
//                    break;
//                }
//                try {
//                    Thread.sleep(1000);
//                } catch (Exception ee) {
//                }
//            }
//
//            channelExec.disconnect();
//        } catch (JSchException | IOException e) {
//
//            e.printStackTrace();
//
//        } finally {
//            if (jschSession != null) {
//                jschSession.disconnect();
//            }
//        }
//
//    }

}
