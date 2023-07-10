package tech.odes.hudi.kafka.starter.common.service;

import tech.odes.hudi.kafka.starter.common.config.ApplicationConfig;
import tech.odes.hudi.kafka.starter.common.config.HudiTable;
import tech.odes.hudi.kafka.starter.common.schema.HoodieTableCodeGenerator;
import tech.odes.hudi.kafka.starter.common.schema.HoodieTableSchemaHelper;
import tech.odes.hudi.kafka.starter.common.schema.KafkaTableCodeGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.*;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import tech.odes.hudi.kafka.starter.common.config.KafkaTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ProceessRunner {

    private static Logger LOG = LoggerFactory.getLogger(ProceessRunner.class);

    private static void registeSource(
            TableEnvironment tableEnv, HudiTable sourceTable) {

        // obtain hudi table schema by storage location
        TableSchema tableSchema = HoodieTableSchemaHelper.getTableSchemaByTablePath(sourceTable.getTablePath());

        // hudi source table code gen
        String connectSourceTableDDL = HoodieTableCodeGenerator.codeGen(sourceTable, tableSchema);

        LOG.info("-------------------------------------------------");
        LOG.info("register hudi source table [{}]: \n {}", sourceTable.getName(), connectSourceTableDDL);
        LOG.info("-------------------------------------------------");

        tableEnv.executeSql(connectSourceTableDDL);

        LOG.info("register hudi source table [{}] success!", sourceTable.getName());
    }

    private static void registeSink(
            TableEnvironment tableEnv, KafkaTable sinkTable, String sinkTableName, TableSchema tableSchema) {

        // kafka sink table code gen
        String connectSinkTableDDL = KafkaTableCodeGenerator.codeGen(sinkTable, sinkTableName, tableSchema);

        LOG.info("-------------------------------------------------");
        LOG.info("register kafka sink table [{}]: \n {}", sinkTableName, connectSinkTableDDL);
        LOG.info("-------------------------------------------------");

        tableEnv.executeSql(connectSinkTableDDL);

        LOG.info("register kafka sink table [{}] success!", sinkTableName);
    }

    /**
     * refactor process base logic and desensitization about column
     *
     * @param tableEnv
     * @param config
     * @return
     */
    private static Table optimizeView(TableEnvironment tableEnv, ApplicationConfig config) {
        // process based transform
        String beforeSensitiveView = StringUtils.isBlank(config.getTransform()) ?
                String.format("select * from %s", config.getSource().getName()) :
                config.getTransform();
        LOG.info("[based transform]: {}", beforeSensitiveView);
        Table tableBeforeSensitive = tableEnv.sqlQuery(beforeSensitiveView);

        if (Objects.isNull(config.getSensitiveColumns()) || config.getSensitiveColumns().size() == 0) {
            LOG.info("none desensitization process");
            return tableBeforeSensitive;
        }

        String viewNameBeforeSensitive = String.format("%s_%s_%s",
                config.getSource().getName(),
                String.valueOf(System.currentTimeMillis()),
                UUID.randomUUID().toString().replaceAll("-", StringUtils.EMPTY));
        tableEnv.createTemporaryView(viewNameBeforeSensitive, tableBeforeSensitive);
        LOG.info("registe based transform to view [{}]", viewNameBeforeSensitive);

        List<TableColumn> tableColumnsBeforeSensitive = tableBeforeSensitive.getSchema().getTableColumns();
        List<String> columnsBeforeSensitive = tableColumnsBeforeSensitive.stream()
                .map(col -> col.getName()).collect(Collectors.toList());
        // check sensitive columns exists
        for (String sensitiveColumn: config.getSensitiveColumns()) {
            if (!columnsBeforeSensitive.contains(sensitiveColumn)) {
                throw new RuntimeException(
                        String.format("Table [%s] has not column [%s]", config.getSource().getName(), sensitiveColumn));
            }
        }

        // process sensitive columns
        List<String> tableAfterSensitive = tableColumnsBeforeSensitive.stream().map(
                col -> config.getSensitiveColumns().contains(col.getName()) ?
                        String.format("%s AS %s",
                                desensitizationFunction(col),
                                String.format("`%s`", col.getName())) :
                        String.format("`%s`", col.getName()))
                .collect(Collectors.toList());
        String afterSensitiveView = String.format("select %s from %s",
                StringUtils.join(tableAfterSensitive, ","), viewNameBeforeSensitive);
        LOG.info("[desensitization transform]: {}", afterSensitiveView);

        return tableEnv.sqlQuery(afterSensitiveView);
    }

    /**
     *
     * simple desensitization function
     *
     * @param column
     *
     * @return
     */
    private static String desensitizationFunction(TableColumn column) {
        LogicalTypeRoot typeRoot = column.getType().getLogicalType().getTypeRoot();
        switch (typeRoot) {
            case DOUBLE:
                return "CAST (-1.0 AS DOUBLE)";
            case FLOAT:
                return "CAST (-1.0 AS FLOAT)";
            case BIGINT:
                return "CAST (-1 AS BIGINT)";
            case BOOLEAN:
                return "false";
            case CHAR:
                return "'*'";
            case INTEGER:
            case TINYINT:
            case SMALLINT:
            case DECIMAL:
                return "CAST (-1 AS INT)";
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TO_TIMESTAMP(FROM_UNIXTIME(0, 'yyyy-MM-dd HH:mm:ss'))";
            case VARCHAR:
            default:
                return "'******'";
        }
    }

    public static void run(TableEnvironment tableEnv, ApplicationConfig config) throws ExecutionException, InterruptedException {
        // 1. registe source table of hudi
        registeSource(tableEnv, config.getSource());

        // 2.view (select)
        Table view = optimizeView(tableEnv, config);

        // 3.registe sink table of kafka
        String kafkaSinkTableName = String.format("kafka_sink_%s_%s", String.valueOf(System.currentTimeMillis()),
                UUID.randomUUID().toString().replaceAll("-", StringUtils.EMPTY));
        registeSink(tableEnv, config.getSink(), kafkaSinkTableName, view.getSchema());

        LOG.info("written data schema------------------------------");
        view.printSchema();
        LOG.info("-------------------------------------------------");

        // 4.sink to kafka
        TableResult tableResult = view.executeInsert(kafkaSinkTableName);
        // wait job finish
        tableResult.getJobClient().get().getJobExecutionResult().get();
    }

}
