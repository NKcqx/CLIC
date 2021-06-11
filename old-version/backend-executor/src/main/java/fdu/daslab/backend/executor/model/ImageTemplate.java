package fdu.daslab.backend.executor.model;

import java.util.List;

/**
 * 标识一个image的实体
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/7 11:37 AM
 */
public class ImageTemplate {

    // platform的名字
    private String platform;

    // template使用的image
    private String image;

    // 运行命令
    private List<String> command;

    // 运行参数，和template有关
    private List<String> args;

    // 运行的参数前缀，有operator有关，拼装operator的前缀
    private String paramPrefix;

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public List<String> getCommand() {
        return command;
    }

    public void setCommand(List<String> command) {
        this.command = command;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public String getParamPrefix() {
        return paramPrefix;
    }

    public void setParamPrefix(String paramPrefix) {
        this.paramPrefix = paramPrefix;
    }
}
