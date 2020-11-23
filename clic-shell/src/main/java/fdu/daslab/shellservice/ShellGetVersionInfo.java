package fdu.daslab.shellservice;

import fdu.daslab.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;

/**
 * 获取当前系统的版本信息
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/10/22 18:15
 */
public class ShellGetVersionInfo {
    private static  Logger logger = LoggerFactory.getLogger(ShellGetVersionInfo.class);
    private static final String VERSION_FILE_NAME =
            System.getProperty("configPath", "version-info.properties");

    public static String getVersion() {
        String version = null;
        try {
            Configuration configuration = new Configuration(VERSION_FILE_NAME);
            version = configuration.getProperty("version-info");

        } catch (FileNotFoundException e) {
            logger.error("An Error occur when shell get CLIC version info from configuration file");
            e.fillInStackTrace();
        }
        return version;
    }
    
    public static void main(String[] args)  {
        logger.info("CLIC version:" + getVersion());
        System.out.println("CLIC " + getVersion());
        System.out.println("other info....");
        System.out.println();
    }
}
