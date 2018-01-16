package io.vertx.kafka.client.admin;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.admin.impl.KafkaAdminImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Vert.x KafkaAdmin
 */
@VertxGen
public interface KafkaAdmin {
  /**
   * Create a new KafkaAdmin instance
   *
   * @param vertx Vert.x instance to use
   * @param config Kafka Admin client configuration
   * @return an instance of the KafkaAdmin
   */
  static KafkaAdmin create(Vertx vertx, Map<String, String> config) {
    return new KafkaAdminImpl(vertx, new HashMap<>(config));
  }

  /**
   * Create a new KafkaAdmin instance
   *
   * @param vertx Vert.x instance to use
   * @param properties Kafka Admin client configuration
   * @return an instance of the KafkaAdmin
   */
  @GenIgnore
  static KafkaAdmin create(Vertx vertx, Properties properties) {
    return new KafkaAdminImpl(vertx, properties);
  }

  /**
   * Creates a new Kafka topic on all Brokers managed by the given Zookeeper instance(s)
   * @param topicName Name of the to-be-created topic
   * @param partitionCount Number of partitions
   * @param replicationFactor Number of replicates. Must be lower or equal to the number of available Brokers
   * @param completionHandler vert.x callback
   */
  void createTopic(String topicName, int partitionCount, short replicationFactor,
                   Handler<AsyncResult<Void>> completionHandler);

  /**
   * Creates a new Kafka topic on all Brokers managed by the given Zookeeper instance(s). In contrast
   * to @see {@link #createTopic(String, int, short, Handler)}, one can pass in additional configuration
   * parameters as a map (String -> String).
   * @param topicName Name of the to-be-created topic
   * @param partitionCount Number of partitions
   * @param replicationFactor Number of replicates. Must be lower or equal to the number of available Brokers
   * @param topicConfig map with additional topic configuration parameters
   * @param completionHandler vert.x callback
   */
  void createTopic(String topicName, int partitionCount, short replicationFactor,
                   Map<String, String> topicConfig,
                   Handler<AsyncResult<Void>> completionHandler);

  /**
   * Delete the Kafka topic given by the topicName.
   * @param topicName Name of the topic to be deleted
   * @param completionHandler vert.x callback
   */
  void deleteTopic(String topicName,
                   Handler<AsyncResult<Void>> completionHandler);

  /**
   * Checks if the Kafka topic given by topicName does exist.
   * @param topicName Name of the topic
   * @param completionHandler vert.x callback
   */
  void topicExists(String topicName,
                   Handler<AsyncResult<Boolean>> completionHandler);

  /**
   * Updates the configuration of the topic given by topicName. Configuration parameters
   * are passed in as a Map (Key -> Value) of Strings.
   * @param topicName topic to be configured
   * @param topicConfig Map with configuration items
   * @param completionHandler vert.x callback
   */
  void changeTopicConfig(String topicName, Map<String, String> topicConfig,
                         Handler<AsyncResult<Void>> completionHandler);

  /**
   * Closes the underlying connection to Zookeeper. It is required to call the method for cleanup
   * purposes if AdminUtils was not created with autoClose set to true.
   * @param completionHandler vert.x callback
   */
  void close(Handler<AsyncResult<Void>> completionHandler);
}
