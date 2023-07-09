$FLINK_HOME/bin/flink run \
-t yarn-per-job \
-d \
-p 2 \
-Djobmanager.memory.process.size=1024mb \
-Dtaskmanager.memory.process.size=2048mb \
-Dtaskmanager.numberOfTaskSlots=2 \
-c tech.odes.hudi.kafka.starter.application.programe.Hoodie2KafkaStreamer \
/opt/hudi-flink-kafka_<hudi-flink-version>.jar \
--config "{...}"