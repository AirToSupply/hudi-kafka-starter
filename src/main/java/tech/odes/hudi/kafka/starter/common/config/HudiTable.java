package tech.odes.hudi.kafka.starter.common.config;

import tech.odes.hudi.kafka.starter.common.schema.HoodieTableSchemaHelper;
import lombok.Builder;
import lombok.experimental.Tolerate;
import org.apache.flink.configuration.Configuration;
import org.apache.hudi.configuration.FlinkOptions;

import java.util.Map;

@Builder
public class HudiTable {

    private String basePath;

    private String targetPath;

    private String name;

    private Map<String, String> conf;

    @Tolerate
    public HudiTable() {
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
    }

    public Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        requireConfiguration(configuration);
        this.conf.entrySet().stream().forEach(item -> configuration.setString(item.getKey(), item.getValue()));
        return configuration;
    }

    private void requireConfiguration(Configuration configuration) {
        configuration.setString(FlinkOptions.PATH, getTablePath());
        configuration.setString(FlinkOptions.TABLE_NAME, this.name);
    }

    public String getTablePath() {
        return HoodieTableSchemaHelper.getTablePath(this.basePath, this.targetPath);
    }
}
