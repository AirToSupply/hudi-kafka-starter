$FLINK_HOME/bin/flink run -d \
-p 2 \
-c tech.odes.hudi.kafka.starter.application.programe.Hoodie2KafkaStreamer \
/opt/hudi-flink-kafka_<hudi-flink-version>.jar \
--config "{...}"