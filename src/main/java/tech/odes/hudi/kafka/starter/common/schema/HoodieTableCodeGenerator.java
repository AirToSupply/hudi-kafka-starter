package tech.odes.hudi.kafka.starter.common.schema;

import tech.odes.hudi.kafka.starter.common.config.HudiTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.hudi.configuration.FlinkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Code Generator DDL about Flink SQL
 */
public class HoodieTableCodeGenerator {

    private static Logger LOG = LoggerFactory.getLogger(HoodieTableCodeGenerator.class);

    public static String codeGen(HudiTable hudiTable, TableSchema schema) {
        Map<String, String> options = hudiTable.getConfiguration().toMap();
        //
        String paritionKeys = options.get(FlinkOptions.PARTITION_PATH_FIELD.key());

        // create table
        String createTable = "create table " + hudiTable.getName() + " (\n";
        StringBuilder builder = new StringBuilder(createTable);

        // table columns
        List<TableColumn> tableColumns = schema.getTableColumns();
        if (Objects.isNull(tableColumns) || tableColumns.size() == 0) {
            LOG.error("Table [{}] has no columns!", hudiTable.getName());
            throw new RuntimeException("There are no columns in the table [" + hudiTable.getName() + "]");
        }

        // find partition columns
        List<TableColumn> tablePartitionColumns = null;
        if (Objects.nonNull(paritionKeys)) {
            List<String> correctPartitionKeys = Arrays.stream(paritionKeys.split(","))
                    .filter(field -> StringUtils.isNotEmpty(field)).collect(Collectors.toList());
            tablePartitionColumns = tableColumns.stream()
                    .filter(column -> correctPartitionKeys.contains(column.getName()))
                    .collect(Collectors.toList());
        }
        if (Objects.nonNull(tablePartitionColumns) && !tablePartitionColumns.isEmpty()) {
            tableColumns.removeAll(tablePartitionColumns);
        }

        // add non-partition columns
        for (int index = 0; index < tableColumns.size(); index++) {
            TableColumn tableColumn = tableColumns.get(index);
            builder.append("  `").append(tableColumn.getName()).append("` ")
                   .append(tableColumn.getType().getLogicalType().asSummaryString());
            if (index != tableColumns.size() - 1) {
                builder.append(",").append("\n");
            }
        }
        // add partition columns
        if (Objects.nonNull(tablePartitionColumns) && !tablePartitionColumns.isEmpty()) {
            for (int index = 0; index < tablePartitionColumns.size(); index++) {
                TableColumn tableColumn = tablePartitionColumns.get(index);
                builder.append(" \n ,`").append(tableColumn.getName()).append("` ")
                        .append(tableColumn.getType().getLogicalType().asSummaryString());
            }
        }
        builder.append("\n)");

        // partition columns
        if (Objects.nonNull(paritionKeys)) {
            // note: Here, we first ignore the partition columns existence judgment,
            //       and let the exception be handled by the Flink execution layer.
            List<String> correctPartitionKeys = Arrays.stream(paritionKeys.split(","))
                    .filter(field -> StringUtils.isNotEmpty(field)).collect(Collectors.toList());

            if (correctPartitionKeys.size() != 0) {
                builder.append(" PARTITIONED BY (");
                builder.append(
                        StringUtils.join(
                                correctPartitionKeys.stream()
                                        .map(f -> String.format("`%s`", f))
                                        .collect(Collectors.toList()),
                                ","));
                builder.append(")\n");
            }
        }

        // table options
        builder.append(" with (\n  'connector' = 'hudi'");
        if (options.size() != 0) {
            options.forEach((k, v) -> builder.append(",\n")
                    .append("  '").append(k).append("' = '").append(v).append("'"));
        }
        builder.append("\n)");

        return builder.toString();
    }

}
