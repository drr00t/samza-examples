package org.apache.samza.quickstart;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.*;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import org.apache.samza.util.CommandLine;

public class WordCount implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:29092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "sample-text";
  private static final String OUTPUT_STREAM_ID = "word-count-output";

  @Override
  public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<KV<String, String>> inputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
            KVSerde.of(new StringSerde(), new StringSerde()));
    KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
            KVSerde.of(new StringSerde(), new StringSerde()));

    MessageStream<KV<String, String>> lines = streamApplicationDescriptor.getInputStream(inputDescriptor);
    OutputStream<KV<String, String>> counts = streamApplicationDescriptor.getOutputStream(outputDescriptor);

    lines
        .map(kv -> kv.value)
        .flatMap(s -> Arrays.asList(s.split("\\W+")))
        .window(Windows.keyedSessionWindow(
            w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
            new StringSerde(), new IntegerSerde()), "count")
        .map(windowPane ->
            KV.of(windowPane.getKey().getKey(),
                windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
        .sendTo(counts);
  }

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Map<String,String> props = new HashMap<>();
    props.put("job.name","word-count");
    props.put("job.coordinator.factory","org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
    props.put("job.coordination.utils.factory","org.apache.samza.standalone.PassthroughCoordinationUtilsFactory");
    props.put("job.changelog.system","kafka");
    props.put("task.name.grouper.factory","org.apache.samza.container.grouper.task.SingleContainerGrouperFactory");
    props.put("processor.id","0");
    props.put("systems.kafka.default.stream.samza.offset.default","oldest");

    Config config =  new MapConfig(props);
    LocalApplicationRunner runner = new LocalApplicationRunner(new WordCount(), config);
    runner.run();
    runner.waitForFinish();
  }
}
