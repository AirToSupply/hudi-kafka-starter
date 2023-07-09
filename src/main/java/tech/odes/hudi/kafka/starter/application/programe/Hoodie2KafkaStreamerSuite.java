package tech.odes.hudi.kafka.starter.application.programe;

import tech.odes.hudi.kafka.starter.common.config.ApplicationConfig;
import tech.odes.hudi.kafka.starter.common.env.ExectionContextFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tech.odes.hudi.kafka.starter.common.env.ExecMode;
import tech.odes.hudi.kafka.starter.common.env.StreamContext;
import tech.odes.hudi.kafka.starter.common.service.ProceessRunner;
import org.yaml.snakeyaml.Yaml;

public class Hoodie2KafkaStreamerSuite {

    private static final String CONFIG_FILE_NAME = "hudi-kafka-sensitive.yaml";

    private static final long CHECKPOINTING_INTERVAL = 1000 * 5L;

    public static void main(String[] args) throws Exception {

        StreamContext exectionContext = (StreamContext) ExectionContextFactory.getExectionContext(ExecMode.STREAM);

        StreamExecutionEnvironment execEnv = exectionContext.getStreamExecutionEnvironment();
        // Note: must be checkpoint
        execEnv.enableCheckpointing(CHECKPOINTING_INTERVAL);
        execEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnv = exectionContext.getStreamTableEnvironment();

        ApplicationConfig applicationConfig = new Yaml().loadAs(Hoodie2KafkaStreamerSuite.class.getClassLoader()
                .getResourceAsStream(CONFIG_FILE_NAME), ApplicationConfig.class);

        ProceessRunner.run(tableEnv, applicationConfig);
    }
}