package io.vertx.kafka.client.admin.impl;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.admin.KafkaAdmin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

@VertxGen
public class KafkaAdminImpl implements KafkaAdmin {
  private Vertx vertx;
  private AdminClient adminClient;

  public KafkaAdminImpl(Vertx vertx, Map<String, Object> config) {
    this.vertx = vertx;
    adminClient = AdminClient.create(config);
  }

  public KafkaAdminImpl(Vertx vertx, Properties properties) {
    this.vertx = vertx;
    adminClient = AdminClient.create(properties);
  }

  @Override
  public void createTopic(String topicName, int partitionCount, short replicationFactor,
                          Handler<AsyncResult<Void>> completionHandler) {
    createTopic(topicName, partitionCount, replicationFactor, new HashMap<>(), completionHandler);
  }

  @Override
  public void createTopic(String topicName, int partitionCount, short replicationFactor,
                          Map<String, String> topicConfig,
                          Handler<AsyncResult<Void>> completionHandler) {
    Properties topicConfigProperties = new Properties();
    topicConfigProperties.putAll(topicConfig);

    vertx.executeBlocking(future -> {
      try {
        NewTopic topic = new NewTopic(topicName, partitionCount, replicationFactor);
        topic.configs(topicConfig);

        adminClient.createTopics(Collections.singletonList(topic)).all().get();
        completionHandler.handle(Future.succeededFuture());
        future.complete();
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
        future.fail(e);
      }
    }, r -> {
    });
  }

  @Override
  public void deleteTopic(String topicName,
                          Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(future -> {
      try {
        adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
        completionHandler.handle(Future.succeededFuture());
        future.complete();
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
        future.fail(e);
      }
    }, r -> {
    });
  }

  @Override
  public void topicExists(String topicName,
                          Handler<AsyncResult<Boolean>> completionHandler) {
    vertx.executeBlocking(future -> {
      try {
        Set<String> topics = adminClient.listTopics().names().get();
        boolean exists = topics.contains(topicName);
        completionHandler.handle(Future.succeededFuture(exists));
        future.complete();
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
        future.fail(e);
      }
    }, r -> {
    });
  }

  @Override
  public void changeTopicConfig(String topicName, Map<String, String> topicConfig,
                          Handler<AsyncResult<Void>> completionHandler) {
    Properties topicConfigProperties = new Properties();
    topicConfigProperties.putAll(topicConfig);

    vertx.executeBlocking(future -> {
      try {
        List<ConfigEntry> config = topicConfig.entrySet().stream()
                  .map(entry -> {return new ConfigEntry(entry.getKey(), entry.getValue());})
                  .collect(Collectors.toList());
        Map<ConfigResource, Config> configs = Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, topicName), new Config(config));

        adminClient.alterConfigs(configs).all().get();
        completionHandler.handle(Future.succeededFuture());
        future.complete();
      } catch(Exception e) {
        completionHandler.handle(Future.failedFuture(e.getLocalizedMessage()));
        future.fail(e);
      }
    }, r -> {
    });
  }

  public void close(Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(future -> {
      adminClient.close();
      completionHandler.handle(Future.succeededFuture());
      future.complete();
    }, r -> {});
  }
}
