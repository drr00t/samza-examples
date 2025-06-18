/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.cookbook;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.cookbook.data.PageView;

import java.util.List;
import java.util.Map;

/**
 * In this example, we demonstrate filtering out some bad events in the stream.
 *
 * <p>Concepts covered: Using stateless operators on a stream.
 *
 * To run the below example:
 *
 * <ol>
 *   <li>
 *     Ensure that the topic "pageview-filter-input" is created  <br/>
 *     ./deploy/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic pageview-filter-input --partitions 2 --replication-factor 1
 *   </li>
 *   <li>
 *     Run the application using the run-app.sh script <br/>
 *     ./deploy/samza/bin/run-app.sh --config-path=$PWD/deploy/samza/config/filter-example.properties
 *   </li>
 *   <li>
 *     Produce some messages to the "pageview-filter-input" topic <br/>
 *     ./deploy/kafka/bin/kafka-console-producer.sh --topic pageview-filter-input --broker-list localhost:9092 <br/>
 *     {"userId": "user1", "country": "india", "pageId":"google.com"} <br/>
 *     {"userId": "invalidUserId", "country": "france", "pageId":"facebook.com"} <br/>
 *     {"userId": "user2", "country": "china", "pageId":"yahoo.com"}
 *   </li>
 *   <li>
 *     Consume messages from the "pageview-filter-output" topic (e.g. bin/kafka-console-consumer.sh)
 *     ./deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pageview-filter-output --property print.key=true
 *   </li>
 * </ol>
 */
public class FilterExample implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:29092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "pageview-filter-input";
  private static final String OUTPUT_STREAM_ID = "pageview-filter-output";
  private static final String INVALID_USER_ID = "invalidUserId";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KVSerde<String, PageView> serde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class));
    KafkaInputDescriptor<KV<String, PageView>> inputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, serde);
    KafkaOutputDescriptor<KV<String, PageView>> outputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, serde);

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    MessageStream<KV<String, PageView>> pageViews = appDescriptor.getInputStream(inputDescriptor);
    OutputStream<KV<String, PageView>> filteredPageViews = appDescriptor.getOutputStream(outputDescriptor);

    pageViews
        .filter(kv -> !INVALID_USER_ID.equals(kv.value.userId))
        .sendTo(filteredPageViews);
  }

  public static void main(String[] args) {
    Config config = new MapConfig(getConfigMap());
    StreamApplication app = new FilterExample();
    LocalApplicationRunner runner = new LocalApplicationRunner(app,config);
    runner.run();
  }

  private static Map<String, String> getConfigMap() {
    return ImmutableMap.of(
        "job.name", "filter-example",
        "job.default.system", KAFKA_SYSTEM_NAME,
        "kafka.consumer.zookeeper.connect", String.join(",", KAFKA_CONSUMER_ZK_CONNECT),
        "kafka.producer.bootstrap.servers", String.join(",", KAFKA_PRODUCER_BOOTSTRAP_SERVERS),
        "kafka.default.stream.replication.factor", KAFKA_DEFAULT_STREAM_CONFIGS.get("replication.factor")
    );
  }
}
