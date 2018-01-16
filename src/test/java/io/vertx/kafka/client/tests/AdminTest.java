package io.vertx.kafka.client.tests;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.kafka.client.admin.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdminTest extends KafkaClusterTestBase {
  private Vertx vertx;
  private String bootstrapServer = "localhost:9092";

  @Before
  public void beforeTest() {
    vertx = Vertx.vertx();
  }

  @After
  public void afterTest(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  KafkaAdmin createAdmin() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", bootstrapServer);
    return KafkaAdmin.create(vertx, config);
  }

  @Test
  public void testCreateTopic(TestContext ctx) throws Exception {
    final String topicName = "testCreateTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaAdmin admin = createAdmin();

    Async createAsync = ctx.async();

    admin.createTopic(topicName, 1, (short)1,
      ctx.asyncAssertSuccess(
      res -> createAsync.complete())
    );

    createAsync.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    admin.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testCreateTopicWithZeroReplicas(TestContext ctx) throws Exception {
    final String topicName = "testCreateTopicWithZeroReplicas";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async async = ctx.async();

    KafkaAdmin admin = createAdmin();

    admin.createTopic(topicName, 1, (short)0,
      ctx.asyncAssertFailure(
        res -> {
          ctx.assertEquals("org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor must be larger than 0.", res.getLocalizedMessage(),
            "Topic creation must fail: one Broker present, but zero replicas requested");
          async.complete();
        })
    );

    async.awaitSuccess(10000);
  }

  @Test
  public void testCreateTopicWithTooManyReplicas(TestContext ctx) throws Exception {
    final String topicName = "testCreateTopicWithTooManyReplicas";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async async = ctx.async();

    KafkaAdmin admin = createAdmin();

    admin.createTopic(topicName, 1, (short)2,
      ctx.asyncAssertFailure(
        res -> {
          ctx.assertEquals("org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 2 larger than available brokers: 1.", res.getLocalizedMessage(),
            "Topic creation must fail: only one Broker present, but two replicas requested");
          async.complete();
        })
    );

    async.awaitSuccess(10000);
  }

  @Test
  public void testCreateExistingTopic(TestContext ctx) throws Exception {
    final String topicName = "testCreateExistingTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async async = ctx.async();

    KafkaAdmin admin = createAdmin();

    admin.createTopic(topicName, 1, (short)1,
      ctx.asyncAssertSuccess(
        res -> async.complete())
    );

    async.awaitSuccess(10000);

    Async create2Async = ctx.async();
    admin.createTopic(topicName, 1, (short)1,
      ctx.asyncAssertFailure(
        res -> {
          ctx.assertEquals("org.apache.kafka.common.errors.TopicExistsException: Topic '"+topicName+"' already exists.", res.getLocalizedMessage(),
            "Topic must already exist");
          create2Async.complete();
        }));

    create2Async.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    admin.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testTopicExists(TestContext ctx) throws Exception {
    final String topicName = "testTopicExists";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async createAsync = ctx.async();

    KafkaAdmin admin = createAdmin();

    admin.createTopic(topicName, 2, (short)1,
      ctx.asyncAssertSuccess(
        res -> createAsync.complete())
    );

    createAsync.awaitSuccess(10000);

    Async existsAndDeleteAsync = ctx.async(2);
    admin.topicExists(topicName, ctx.asyncAssertSuccess(res -> existsAndDeleteAsync.countDown()));
    admin.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> existsAndDeleteAsync.countDown()));

    existsAndDeleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testTopicExistsNonExisting(TestContext ctx) throws Exception {
    final String topicName = "testTopicExistsNonExisting";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async createAsync = ctx.async();

    KafkaAdmin admin = createAdmin();

    admin.topicExists(topicName, ctx.asyncAssertSuccess(res -> {
        ctx.assertFalse(res, "Topic must not exist");
        createAsync.complete();
      })
    );
    createAsync.awaitSuccess(10000);
  }

  @Test
  public void testDeleteTopic(TestContext ctx) throws Exception {
    final String topicName = "testDeleteTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    Async async = ctx.async();

    KafkaAdmin admin = createAdmin();

    admin.createTopic(topicName, 1, (short)1,
      ctx.asyncAssertSuccess(
        res -> async.complete())
    );

    async.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    admin.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);

    Async existsAsync = ctx.async();
    admin.topicExists(topicName, ctx.asyncAssertSuccess(res -> {
        ctx.assertFalse(res, "Topic must not exist");
      existsAsync.complete();
      })
    );
    existsAsync.awaitSuccess(10000);
  }

  @Test
  public void testDeleteNonExistingTopic(TestContext ctx) throws Exception {
    final String topicName = "testDeleteNonExistingTopic";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaAdmin admin = createAdmin();

    Async async = ctx.async();

    admin.deleteTopic(topicName, ctx.asyncAssertFailure(res -> {
        ctx.assertEquals("org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.", res.getLocalizedMessage(),
          "Topic must not exist (not created before)");
        async.complete();
      })
    );
  }

  @Test
  public void testChangeTopicConfig(TestContext ctx) throws Exception {
    final String topicName = "testChangeTopicConfig";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaAdmin admin = createAdmin();

    Async createAsync = ctx.async();

    admin.createTopic(topicName, 2, (short)1,
      ctx.asyncAssertSuccess(
        res -> createAsync.complete()));

    createAsync.awaitSuccess(10000);

    Async changeAsync = ctx.async();
    Map<String, String> properties = new HashMap<>();
    properties.put("delete.retention.ms", "1000");
    properties.put("retention.bytes", "1024");
    admin.changeTopicConfig(topicName, properties,
      ctx.asyncAssertSuccess(res -> changeAsync.complete())
    );

    changeAsync.awaitSuccess(10000);

    Async deleteAsync = ctx.async();
    admin.deleteTopic(topicName, ctx.asyncAssertSuccess(res -> deleteAsync.complete()));
    deleteAsync.awaitSuccess(10000);
  }

  @Test
  public void testChangeTopicConfigWrongConfig(TestContext ctx) throws Exception {
    final String topicName = "testChangeTopicConfigWrongConfig";
    Properties config = kafkaCluster.useTo().getProducerProperties("the_producer");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaAdmin admin = createAdmin();

    Async createAsync = ctx.async();
    admin.createTopic(topicName, 2, (short)1,
      ctx.asyncAssertSuccess(
        res -> createAsync.complete())
    );

    createAsync.awaitSuccess(10000);

    Async async = ctx.async();
    Map<String, String> properties = new HashMap<>();
    properties.put("this.does.not.exist", "1024L");

    admin.changeTopicConfig(topicName, properties, ctx.asyncAssertFailure(res -> {
        ctx.assertEquals("org.apache.kafka.common.errors.InvalidConfigurationException: Unknown topic config name: this.does.not.exist", res.getLocalizedMessage());
        async.complete();
      })
    );
  }
}
