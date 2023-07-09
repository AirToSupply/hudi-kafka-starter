package tech.odes.hudi.kafka.starter.common.schema;

import tech.odes.hudi.kafka.starter.common.config.KafkaTable;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Code Generator DDL about Flink SQL
 */
public class KafkaTableCodeGenerator {

    private static Logger LOG = LoggerFactory.getLogger(KafkaTableCodeGenerator.class);

    public static String codeGen(KafkaTable conf, String kafkaTableName, TableSchema schema) {

        // create table
        String createTable = "create table " + kafkaTableName + " (\n";
        StringBuilder builder = new StringBuilder(createTable);

        // table columns
        List<TableColumn> tableColumns = schema.getTableColumns();
        if (Objects.isNull(tableColumns) || tableColumns.size() == 0) {
            LOG.error("Table [{}] has no columns!", kafkaTableName);
            throw new RuntimeException("There are no columns in the table [" + kafkaTableName + "]");
        }

        for (int index = 0; index < tableColumns.size(); index++) {
            TableColumn tableColumn = tableColumns.get(index);
            builder.append("  `").append(tableColumn.getName()).append("` ")
                   .append(tableColumn.getType().getLogicalType().asSummaryString());
            if (index != tableColumns.size() - 1) {
                builder.append(",").append("\n");
            }
        }
        builder.append("\n)");

        Map<String, String> options = conf.getConf();

        // table options
        builder.append("\nwith (\n  'connector' = 'kafka'");
        if (options.size() != 0) {
            options.forEach((k, v) -> builder.append(",\n")
                    .append("  '").append(k).append("' = '").append(v).append("'"));
        }
        builder.append("\n)");

        return builder.toString();
    }

}
