package tech.odes.hudi.kafka.starter.common.config;

import java.util.List;

public class ApplicationConfig {

    private HudiTable source;

    private KafkaTable sink;

    private String transform;

    private List<String> sensitiveColumns;

    public ApplicationConfig() {}

    public HudiTable getSource() {
        return source;
    }

    public void setSource(HudiTable source) {
        this.source = source;
    }

    public KafkaTable getSink() {
        return sink;
    }

    public void setSink(KafkaTable sink) {
        this.sink = sink;
    }

    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }

    public List<String> getSensitiveColumns() {
        return sensitiveColumns;
    }

    public void setSensitiveColumns(List<String> sensitiveColumns) {
        this.sensitiveColumns = sensitiveColumns;
    }
}
