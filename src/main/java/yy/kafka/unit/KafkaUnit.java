/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package yy.kafka.unit;

import kafka.admin.TopicCommand;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ComparisonFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Console;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaUnit
{

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUnit.class);

  private KafkaServerStartable broker;

  private Zookeeper zookeeper;
  private final String zookeeperString;
  private final String brokerString;
  private int zkPort;
  private int brokerPort;
  private org.apache.kafka.clients.producer.Producer<String, String> producer = null;
  private Properties kafkaBrokerConfig = new Properties();

  public KafkaUnit() throws IOException
  {
    this(getEphemeralPort(), getEphemeralPort());
  }

  public KafkaUnit(int zkPort, int brokerPort)
  {
    this.zkPort = zkPort;
    this.brokerPort = brokerPort;
    this.zookeeperString = "localhost:" + zkPort;
    this.brokerString = "localhost:" + brokerPort;
  }

  public KafkaUnit(String zkConnectionString, String kafkaConnectionString)
  {
    this(parseConnectionString(zkConnectionString), parseConnectionString(kafkaConnectionString));
  }

  private static int parseConnectionString(String connectionString)
  {
    try
    {
      String[] hostPorts = connectionString.split(",");

      if (hostPorts.length != 1)
      {
        throw new IllegalArgumentException("Only one 'host:port' pair is allowed in connection string");
      }

      String[] hostPort = hostPorts[0].split(":");

      if (hostPort.length != 2)
      {
        throw new IllegalArgumentException("Invalid format of a 'host:port' pair");
      }

      if (!"localhost".equals(hostPort[0]))
      {
        throw new IllegalArgumentException("Only localhost is allowed for KafkaUnit");
      }

      return Integer.parseInt(hostPort[1]);
    }
    catch (Exception e)
    {
      throw new RuntimeException("Cannot parse connectionString " + connectionString, e);
    }
  }

  private static int getEphemeralPort() throws IOException
  {
    try (ServerSocket socket = new ServerSocket(0))
    {
      return socket.getLocalPort();
    }
  }

  public void startup()
  {
    zookeeper = new Zookeeper(zkPort);
    zookeeper.startup();

    final File logDir = LogDirUtil.prepareLogDir("kafka");
    kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperString);
    kafkaBrokerConfig.setProperty("broker.id", "1");
    kafkaBrokerConfig.setProperty("host.name", "localhost");
    kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
    kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
    kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
    kafkaBrokerConfig.setProperty("delete.topic.enable", String.valueOf(true));

    broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig));
    broker.startup();
  }

  public String getKafkaConnect()
  {
    return brokerString;
  }

  public int getZkPort()
  {
    return zkPort;
  }

  public int getBrokerPort()
  {
    return brokerPort;
  }

  public void createTopic(String topicName)
  {
    createTopic(topicName, 1);
  }

  public void createTopic(String topicName, Integer numPartitions)
  {
    // setup
    String[] arguments = new String[9];
    arguments[0] = "--create";
    arguments[1] = "--zookeeper";
    arguments[2] = zookeeperString;
    arguments[3] = "--replication-factor";
    arguments[4] = "1";
    arguments[5] = "--partitions";
    arguments[6] = "" + Integer.valueOf(numPartitions);
    arguments[7] = "--topic";
    arguments[8] = topicName;
    TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

    ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
            30000, 30000, JaasUtils.isZkSecurityEnabled());

    try
    {
      // run
      LOGGER.info("Executing: CreateTopic " + Arrays.toString(arguments));
      TopicCommand.createTopic(zkUtils, opts);
    }
    finally
    {
      zkUtils.close();
    }
  }

  /**
   * @return All topic names
   */
  public List<String> listTopics()
  {
    String[] arguments = new String[3];
    arguments[0] = "--zookeeper";
    arguments[1] = zookeeperString;
    arguments[2] = "--list";
    TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

    ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
            30000, 30000, JaasUtils.isZkSecurityEnabled());
    final List<String> topics = new ArrayList<>();
    try
    {
      // run
      LOGGER.info("Executing: ListTopics " + Arrays.toString(arguments));

      PrintStream oldOut = Console.out();
      try
      {
        Console.setOut(new PrintStream(oldOut)
        {
          @Override
          public void print(String s)
          {
            super.print(s);
            if (!s.endsWith("marked for deletion"))
            {
              topics.add(s);
            }
          }
        });
        TopicCommand.listTopics(zkUtils, opts);
      }
      finally
      {
        Console.setOut(oldOut);
      }
    }
    finally
    {
      zkUtils.close();
    }

    return topics;
  }

  /**
   * Delete all topics
   */
  public void deleteAllTopics()
  {
    for (String topic : listTopics())
    {
      try
      {
        deleteTopic(topic);
      }
      catch (Throwable ignored)
      {
      }
    }
  }

  /**
   * Delete a topic.
   *
   * @param topicName The name of the topic to delete
   */
  public void deleteTopic(String topicName)
  {
    String[] arguments = new String[5];
    arguments[0] = "--zookeeper";
    arguments[1] = zookeeperString;
    arguments[2] = "--delete";
    arguments[3] = "--topic";
    arguments[4] = topicName;
    TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

    ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
            30000, 30000, JaasUtils.isZkSecurityEnabled());
    try
    {
      // run
      LOGGER.info("Executing: DeleteTopic " + Arrays.toString(arguments));
      TopicCommand.deleteTopic(zkUtils, opts);
    }
    finally
    {
      zkUtils.close();
    }
  }

  public void shutdown()
  {
    if (broker != null)
    {
      broker.shutdown();
      broker.awaitShutdown();
    }
    if (zookeeper != null)
    {
      zookeeper.shutdown();
    }
  }

  public List<ProducerRecord<String, String>> readProducerRecords(final String topicName, final int expectedMessages) throws TimeoutException
  {
    return readMessages(topicName, expectedMessages,
            messageAndMetadata -> new ProducerRecord<>(topicName, messageAndMetadata.key(),
                    messageAndMetadata.message()));
  }

  public List<String> readMessages(String topicName, final int expectedMessages) throws TimeoutException
  {
    return readMessages(topicName, expectedMessages, messageAndMetadata -> messageAndMetadata.message());
  }

  private <T> List<T> readMessages(String topicName, final int expectedMessages, final MessageExtractor<T> messageExtractor) throws TimeoutException
  {
    ExecutorService singleThread = Executors.newSingleThreadExecutor();
    Properties consumerProperties = new Properties();
    consumerProperties.put("zookeeper.connect", zookeeperString);
    consumerProperties.put("group.id", "10");
    consumerProperties.put("socket.timeout.ms", "500");
    consumerProperties.put("consumer.id", "test");
    consumerProperties.put("auto.offset.reset", "smallest");
    consumerProperties.put("consumer.timeout.ms", "500");
    ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(
            new ConsumerConfig(consumerProperties));
    StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties(new Properties()));
    Map<String, Integer> topicMap = new HashMap<>();
    topicMap.put(topicName, 1);
    Map<String, List<KafkaStream<String, String>>> events = javaConsumerConnector.createMessageStreams(topicMap,
            stringDecoder, stringDecoder);
    List<KafkaStream<String, String>> events1 = events.get(topicName);
    final KafkaStream<String, String> kafkaStreams = events1.get(0);


    Future<List<T>> submit = singleThread.submit(() ->
    {
      List<T> messages = new ArrayList<>();
      try
      {
        for (MessageAndMetadata<String, String> kafkaStream : kafkaStreams)
        {
          T message = messageExtractor.extract(kafkaStream);
          LOGGER.info("Received message: {}", kafkaStream.message());
          messages.add(message);
        }
      }
      catch (ConsumerTimeoutException e)
      {
        // always gets throws reaching the end of the stream
      }
      if (expectedMessages >= 0 && messages.size() != expectedMessages)
      {
        throw new ComparisonFailure("Incorrect number of messages returned", Integer.toString(expectedMessages),
                Integer.toString(messages.size()));
      }
      return messages;
    });

    try
    {
      return submit.get(3, TimeUnit.SECONDS);
    }
    catch (InterruptedException | ExecutionException | TimeoutException e)
    {
      if (e.getCause() instanceof ComparisonFailure)
      {
        throw (ComparisonFailure) e.getCause();
      }
      throw new TimeoutException("Timed out waiting for messages");
    }
    finally
    {
      singleThread.shutdown();
      javaConsumerConnector.shutdown();
    }
  }

  public List<String> pollMessages(String topicName) throws TimeoutException
  {
    return readMessages(topicName, -1, messageAndMetadata -> messageAndMetadata.message());
  }

  public final void sendMessages(ProducerRecord<String, String> message)
  {
    if (producer == null)
    {
      Properties props = new Properties();
      props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(BOOTSTRAP_SERVERS_CONFIG, brokerString);
      producer = new KafkaProducer<>(props);
    }
    producer.send(message);
    producer.close();
  }

  public final void sendMessages(List<ProducerRecord<String, String>> messages)
  {
    if (producer == null)
    {
      Properties props = new Properties();
      props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(BOOTSTRAP_SERVERS_CONFIG, brokerString);
      producer = new KafkaProducer<>(props);
    }

    for (ProducerRecord<String, String> msg : messages)
    {
      producer.send(msg);
    }
    producer.close();
  }

  /**
   * Set custom broker configuration.
   * See available config keys in the kafka documentation: http://kafka.apache.org/documentation.html#brokerconfigs
   */
  public final void setKafkaBrokerConfig(String configKey, String configValue)
  {
    kafkaBrokerConfig.setProperty(configKey, configValue);
  }

  private interface MessageExtractor<T>
  {
    T extract(MessageAndMetadata<String, String> messageAndMetadata);
  }
}

