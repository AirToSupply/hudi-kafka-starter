package tech.odes.hudi.kafka.starter.common.config;

import lombok.Builder;
import lombok.experimental.Tolerate;

import java.util.Map;

@Builder
public class KafkaTable {

    private Map<String, String> conf;

    public static final String __TOPIC = "topic";

    @Tolerate
    public KafkaTable() {
    }

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
    }
}
