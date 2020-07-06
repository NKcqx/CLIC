package basic;

import org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Configuration {
    private static final String DEFAULT_CONFIGURATION_FILE_NAME = "default-config.properties";
    private Map<String, String> properties = new HashMap<>();

    public Configuration() throws FileNotFoundException {
        this(DEFAULT_CONFIGURATION_FILE_NAME);
    }

    public Configuration(String configFileName) throws FileNotFoundException {
        try {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream(configFileName);
            this.loadConfig(in);
        } catch (Exception e) {
            throw new FileNotFoundException(String.format("无法加载默认配置文件，请检查是否存在: ", DEFAULT_CONFIGURATION_FILE_NAME));
        }
    }

    private void loadConfig(InputStream configStream) throws FileNotFoundException {
        try {
            final Properties configProperties = new Properties();
            configProperties.load(configStream);
            for (Map.Entry<Object, Object> propertyEntry : configProperties.entrySet()) {
                final String key = propertyEntry.getKey().toString();
                final String value = propertyEntry.getValue().toString();
                this.properties.put(key, value);
            }
        } catch (IOException e) {
            throw new FileNotFoundException(String.format("无法加载默认配置文件，请检查是否存在: ", DEFAULT_CONFIGURATION_FILE_NAME));
        } finally {
            IOUtils.closeQuietly(configStream);
        }
    }

    public String getProperty(String key) {
        return this.properties.get(key);
    }
}
