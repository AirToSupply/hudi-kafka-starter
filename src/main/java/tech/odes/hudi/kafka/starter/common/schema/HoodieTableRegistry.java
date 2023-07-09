package tech.odes.hudi.kafka.starter.common.schema;

import tech.odes.hudi.kafka.starter.common.config.HudiTable;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodieTableRegistry {

    private static Logger LOG = LoggerFactory.getLogger(HoodieTableRegistry.class);

    public static void connect(TableEnvironment tableEnv, HudiTable hudiTable) {

        TableSchema tableSchema = HoodieTableSchemaHelper.getTableSchemaByTablePath(hudiTable.getTablePath());

        // hudi source table code gen
        String connectSourceTableDDL = HoodieTableCodeGenerator.codeGen(hudiTable, tableSchema);

        LOG.info("-------------------------------------------------");
        LOG.info("register hudi table [{}]: \n {}", hudiTable.getName(), connectSourceTableDDL);
        LOG.info("-------------------------------------------------");

        tableEnv.executeSql(connectSourceTableDDL);

        LOG.info("register hudi table [{}] success!", hudiTable.getName());
    }
}
