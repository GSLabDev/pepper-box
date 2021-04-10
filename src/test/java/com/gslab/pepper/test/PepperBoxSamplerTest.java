package com.gslab.pepper.test;

import com.gslab.pepper.config.plaintext.PlainTextConfigElement;
import com.gslab.pepper.config.serialized.SerializedConfigElement;
import com.gslab.pepper.model.FieldExpressionMapping;
import com.gslab.pepper.sampler.PepperBoxKafkaSampler;
import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Time;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by satish on 5/3/17.
 */
public class PepperBoxSamplerTest {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "test";

    private EmbeddedZookeeper zkServer = null;

    private KafkaServer kafkaServer = null;

    private ZkClient zkClient = null;

    private  JavaSamplerContext jmcx = null;

    @Before
    public void setup() throws IOException {

        zkServer = new EmbeddedZookeeper();

        String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        //AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        JMeterContext jmcx = JMeterContextService.getContext();
        jmcx.setVariables(new JMeterVariables());

    }

    @Test
    @Ignore
    public void plainTextSamplerTest() throws IOException {

        PepperBoxKafkaSampler sampler = new PepperBoxKafkaSampler();
        Arguments arguments = sampler.getDefaultParameters();
        arguments.removeArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        arguments.removeArgument(ProducerKeys.KAFKA_TOPIC_CONFIG);
        arguments.removeArgument(ProducerKeys.ZOOKEEPER_SERVERS);
        arguments.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
        arguments.addArgument(ProducerKeys.ZOOKEEPER_SERVERS, ZKHOST + ":" + zkServer.port());
        arguments.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, TOPIC);

        jmcx = new JavaSamplerContext(arguments);
        sampler.setupTest(jmcx);

        PlainTextConfigElement plainTextConfigElement = new PlainTextConfigElement();
        plainTextConfigElement.setJsonSchema(TestInputUtils.testSchema);
        plainTextConfigElement.setPlaceHolder(PropsKeys.MSG_PLACEHOLDER);
        plainTextConfigElement.iterationStart(null);

        Object msgSent = JMeterContextService.getContext().getVariables().getObject(PropsKeys.MSG_PLACEHOLDER);
        sampler.runTest(jmcx);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        ConsumerRecords<String, String> records = consumer.poll(30000);
        Assert.assertEquals(1, records.count());
        for (ConsumerRecord<String, String> record : records){
            Assert.assertEquals("Failed to validate produced message", msgSent.toString(), record.value());
        }

        sampler.teardownTest(jmcx);

    }

    @Test
    @Ignore
    public void serializedSamplerTest() throws IOException {

        PepperBoxKafkaSampler sampler = new PepperBoxKafkaSampler();
        Arguments arguments = sampler.getDefaultParameters();
        arguments.removeArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        arguments.removeArgument(ProducerKeys.KAFKA_TOPIC_CONFIG);
        arguments.removeArgument(ProducerKeys.ZOOKEEPER_SERVERS);
        arguments.removeArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        arguments.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, TOPIC);
        arguments.addArgument(ProducerKeys.ZOOKEEPER_SERVERS, ZKHOST + ":" + zkServer.port());
        arguments.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
        arguments.addArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.gslab.pepper.input.serialized.ObjectSerializer");

        jmcx = new JavaSamplerContext(arguments);
        sampler.setupTest(jmcx);

        List<FieldExpressionMapping> fieldExpressionMappings = TestInputUtils.getFieldExpressionMappings();
        SerializedConfigElement serializedConfigElement = new SerializedConfigElement();
        serializedConfigElement.setClassName("com.gslab.pepper.test.Message");
        serializedConfigElement.setObjProperties(fieldExpressionMappings);
        serializedConfigElement.setPlaceHolder(PropsKeys.MSG_PLACEHOLDER);
        serializedConfigElement.iterationStart(null);

        Message msgSent = (Message) JMeterContextService.getContext().getVariables().getObject(PropsKeys.MSG_PLACEHOLDER);
        sampler.runTest(jmcx);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "com.gslab.pepper.input.serialized.ObjectDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        ConsumerRecords<String, Message> records = consumer.poll(30000);
        Assert.assertEquals(1, records.count());
        for (ConsumerRecord<String, Message> record : records){
            Assert.assertEquals("Failed to validate produced message", msgSent.getMessageBody(), record.value().getMessageBody());
        }

        sampler.teardownTest(jmcx);

    }

    @After
    public void teardown(){
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();

    }

}
