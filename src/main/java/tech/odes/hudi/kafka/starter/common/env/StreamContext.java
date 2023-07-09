package tech.odes.hudi.kafka.starter.common.env;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamContext implements ExectionContext {

    private StreamExecutionEnvironment streamExecutionEnvironment;

    private StreamTableEnvironment streamTableEnvironment;

    public StreamContext(StreamExecutionEnvironment streamExecutionEnvironment, StreamTableEnvironment streamTableEnvironment) {
        this.streamExecutionEnvironment = streamExecutionEnvironment;
        this.streamTableEnvironment = streamTableEnvironment;
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return streamExecutionEnvironment;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return streamTableEnvironment;
    }
}
