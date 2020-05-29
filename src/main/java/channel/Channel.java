package channel;

/**
 * @description：
 */
public abstract class Channel {

    private String filePath;

    public Channel(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
