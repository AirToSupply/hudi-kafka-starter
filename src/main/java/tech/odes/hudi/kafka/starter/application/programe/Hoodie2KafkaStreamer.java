package tech.odes.hudi.kafka.starter.application.programe;

import com.alibaba.fastjson.JSON;
import tech.odes.hudi.kafka.starter.common.config.ApplicationConfig;
import tech.odes.hudi.kafka.starter.common.config.KafkaTable;
import tech.odes.hudi.kafka.starter.common.env.ExecMode;
import tech.odes.hudi.kafka.starter.common.env.ExectionContextFactory;
import tech.odes.hudi.kafka.starter.common.env.StreamContext;
import tech.odes.hudi.kafka.starter.common.service.ProceessRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * hudi to kafka with Flink SQL
 */
public class Hoodie2KafkaStreamer {

    private static Logger LOG = LoggerFactory.getLogger(Hoodie2KafkaStreamer.class);

    private static final String OPTION_CONFIG = "config";

    private static final String OPTION_CHECKPOINTING_INTERVAL = "checkpoint-interval";

    private static final long DEFAULT_CHECKPOINTING_INTERVAL = 1000 * 5L;

    private static final String PIPELINE_NAME_FORMAT = "[Hoodie:%s] => [Kafka:%s]";

    private static final String __YARN_APPLICATION_NAME = "yarn.application.name";

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        String config = parameters.get(OPTION_CONFIG, null);

        if (StringUtils.isBlank(config)) {
            throw new RuntimeException("arguments [" + OPTION_CONFIG + "] must be setting!");
        }

        ApplicationConfig applicationConfig = null;

        try {
            LOG.info("arguments [" + OPTION_CONFIG + "] : {}", config);
            applicationConfig = JSON.parseObject(config, ApplicationConfig.class);
        } catch (Exception ex) {
            throw new RuntimeException("arguments [" + OPTION_CONFIG + "] error!");
        }

        final String applicationName = String.format(PIPELINE_NAME_FORMAT, applicationConfig.getSource().getName(),
            applicationConfig.getSink().getConf().getOrDefault(KafkaTable.__TOPIC, StringUtils.EMPTY));

        final Configuration configuration = Configuration.fromMap(Collections.singletonMap(__YARN_APPLICATION_NAME, applicationName));
        StreamContext exectionContext = (StreamContext) ExectionContextFactory.getExectionContext(ExecMode.STREAM, configuration);
        StreamExecutionEnvironment execEnv = exectionContext.getStreamExecutionEnvironment();

        // Note: must be checkpoint
        execEnv.enableCheckpointing(parameters.getLong(OPTION_CHECKPOINTING_INTERVAL, DEFAULT_CHECKPOINTING_INTERVAL));
        execEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // job name setting
        // yarn
        execEnv.getConfig().configure(configuration, Hoodie2KafkaStreamer.class.getClassLoader());
        // standalone
        final StreamTableEnvironment tableEnv = exectionContext.getStreamTableEnvironment();
        tableEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, applicationName);

        ProceessRunner.run(tableEnv, applicationConfig);
    }
}