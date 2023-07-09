package tech.odes.hudi.kafka.starter.common.env;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

public class BatchContext implements ExectionContext {

    private StreamExecutionEnvironment streamExecutionEnvironment;

    private TableEnvironment tableEnvironment;

    public BatchContext(StreamExecutionEnvironment streamExecutionEnvironment, TableEnvironment tableEnvironment) {
        this.streamExecutionEnvironment = streamExecutionEnvironment;
        this.tableEnvironment = tableEnvironment;
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return streamExecutionEnvironment;
    }

    public TableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }
}
