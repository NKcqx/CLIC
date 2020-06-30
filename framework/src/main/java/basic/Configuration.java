package basic;

import org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Configuration {
    private static final String DEFAULT_CONFIGURATION_FILE_NAME = "default-config.properties";
    private Map<String, String> properties = new HashMap<>();

    public Configuration() throws FileNotFoundException {
        this(DEFAULT_CONFIGURATION_FILE_NAME);
    }

    public Configuration(String config_file_name) throws FileNotFoundException {
        try {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream(config_file_name);
            this.loadConfig(in);
        }catch (Exception e){
            throw new FileNotFoundException(String.format("无法加载默认配置文件，请检查是否存在: ", DEFAULT_CONFIGURATION_FILE_NAME));
        }
    }

    private void loadConfig(InputStream config_stream) throws FileNotFoundException {
        try {
            final Properties properties = new Properties();
            properties.load(config_stream);
            for (Map.Entry<Object, Object> propertyEntry : properties.entrySet()) {
                final String key = propertyEntry.getKey().toString();
                final String value = propertyEntry.getValue().toString();
                this.properties.put(key, value);
            }
        } catch (IOException e) {
            throw new FileNotFoundException(String.format("无法加载默认配置文件，请检查是否存在: ", DEFAULT_CONFIGURATION_FILE_NAME));
        } finally {
            IOUtils.closeQuietly(config_stream);
        }
    }
    public String getProperty(String key){
        return this.properties.get(key);
    }
}
