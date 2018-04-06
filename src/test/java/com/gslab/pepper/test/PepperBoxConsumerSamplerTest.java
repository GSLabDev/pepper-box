//package com.gslab.pepper.test;
//
//import static org.junit.Assert.assertTrue;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.util.Collections;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//
//import org.I0Itec.zkclient.ZkClient;
//import org.apache.jmeter.config.Arguments;
//import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
//import org.apache.jmeter.threads.JMeterContext;
//import org.apache.jmeter.threads.JMeterContextService;
//import org.apache.jmeter.threads.JMeterVariables;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.utils.Time;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.ClassRule;
//import org.junit.Test;
//import org.springframework.kafka.test.rule.KafkaEmbedded;
//import org.springframework.kafka.test.utils.KafkaTestUtils;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaProducerFactory;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.core.ProducerFactory;
//import org.springframework.kafka.listener.AcknowledgingMessageListener;
//import org.springframework.kafka.listener.KafkaMessageListenerContainer;
//import org.springframework.kafka.listener.config.ContainerProperties;
//
//import com.gslab.pepper.config.plaintext.PlainTextConfigElement;
//import com.gslab.pepper.sampler.PepperBoxKafkaConsumerSampler;
//import com.gslab.pepper.sampler.PepperBoxKafkaSampler;
//import com.gslab.pepper.util.ProducerKeys;
//import com.gslab.pepper.util.PropsKeys;
//
//import kafka.admin.AdminUtils;
//import kafka.admin.RackAwareMode;
//import kafka.server.KafkaConfig;
//import kafka.server.KafkaServer;
//import kafka.utils.MockTime;
//import kafka.utils.TestUtils;
//import kafka.utils.ZKStringSerializer$;
//import kafka.utils.ZkUtils;
//import kafka.zk.EmbeddedZookeeper;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class PepperBoxConsumerSamplerTest {
//
//	private static final String ZKHOST = "127.0.0.1";
//	private static final String BROKERHOST = "127.0.0.1";
//	private static final int BROKERPORT = 9092;
//	private static final String TOPIC = "test";
//
//	private EmbeddedZookeeper zkServer = null;
//
//	private KafkaServer kafkaServer = null;
//
//	private ZkClient zkClient = null;
//
//	private  JavaSamplerContext jmcx = null;
//
//    @ClassRule
//    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 2, "messages");
//    
//    private static final Logger LOGGER = LoggerFactory.getLogger(PepperBoxConsumerSamplerTest.class);
//    
//	@Before
//	public void setup() throws IOException {
//	    
//		zkServer = new EmbeddedZookeeper();
//
//		String zkConnect = ZKHOST + ":" + zkServer.port();
//		zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
//		Properties brokerProps = new Properties();
//		brokerProps.setProperty("zookeeper.connect", zkConnect);
//		brokerProps.setProperty("broker.id", "0");
//		brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
//		brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
//		//brokerProps.setProperty("listeners", "PLAINTEXT://127.0.0.1:9091,PLAINTEXT://127.0.0.2:9092,PLAINTEXT://127.0.0.3:9093");
//		KafkaConfig config = new KafkaConfig(brokerProps);
//		//offsets.topic.replication.factor
//		//config.offsetsTopicReplicationFactor();
//		Time mock = new MockTime();
//		kafkaServer = TestUtils.createServer(config, mock);
//
//		ZkUtils zkUtils = new ZkUtils(zkClient,null, false) ;
//		AdminUtils.createTopic(zkUtils, TOPIC, 3, 3, new Properties(), RackAwareMode.Disabled$.MODULE$);
//
//		JMeterContext jmcx = JMeterContextService.getContext();
//		jmcx.setVariables(new JMeterVariables());
//
//	}
//
//	@Test
//	public void plainTextSamplerTest() throws IOException {
//
///*		PepperBoxKafkaSampler sampler = new PepperBoxKafkaSampler();
//		PepperBoxKafkaConsumerSampler cSampler = new PepperBoxKafkaConsumerSampler();
//		Arguments arguments = sampler.getDefaultParameters();
//		arguments.removeArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
//		arguments.removeArgument(ProducerKeys.KAFKA_TOPIC_CONFIG);
//		arguments.removeArgument(ProducerKeys.ZOOKEEPER_SERVERS);
//		arguments.removeArgument(ConsumerConfig.GROUP_ID_CONFIG);
//		arguments.removeArgument(ConsumerConfig.CLIENT_ID_CONFIG);
//		arguments.removeArgument(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
//		arguments.removeArgument(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
//		arguments.removeArgument(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
//		arguments.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
//		arguments.addArgument(ProducerKeys.ZOOKEEPER_SERVERS, ZKHOST + ":" + zkServer.port());
//		arguments.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, TOPIC);
//
//		arguments.removeArgument(ConsumerConfig.GROUP_ID_CONFIG);
//		arguments.removeArgument(ConsumerConfig.CLIENT_ID_CONFIG);
//		arguments.removeArgument(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
//		arguments.removeArgument(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
//		arguments.removeArgument(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
//		arguments.addArgument(ConsumerConfig.GROUP_ID_CONFIG, "group0");
//		arguments.addArgument(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
//		arguments.addArgument(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
//		arguments.addArgument(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//		arguments.addArgument(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//
//		PlainTextConfigElement plainTextConfigElement = new PlainTextConfigElement();
//		plainTextConfigElement.setJsonSchema(TestInputUtils.testSchema);
//		plainTextConfigElement.setPlaceHolder(PropsKeys.MSG_PLACEHOLDER);
//		plainTextConfigElement.iterationStart(null);
//
//		jmcx = new JavaSamplerContext(arguments);
//		sampler.setupTest(jmcx);
//		sampler.runTest(jmcx);
//
//		cSampler.setupTest(jmcx);
//		cSampler.runTest(jmcx);
//		try {}catch(Exception e) {
//		}finally {
//			sampler.teardownTest(jmcx);
//			cSampler.teardownTest(jmcx);
//		}*/
//	}
//	
//    @Test
//    public void testSpringKafka() throws Exception {
//        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleConsumer", "false", embeddedKafka);
//        consumerProps.put("auto.offset.reset", "earliest");
//        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
//        ContainerProperties containerProps = new ContainerProperties("messages");
//
//        final CountDownLatch latch = new CountDownLatch(4);
//        containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
//            LOGGER.info("Receiving: " + message);
//            try {
//                Thread.sleep(200);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//            latch.countDown();
//        });
//        KafkaMessageListenerContainer<Integer, String> container =
//                new KafkaMessageListenerContainer<>(cf, containerProps);
//        container.setBeanName("sampleConsumer");
//
//
//        container.start();
////        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
//
//        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
//        ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
//        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
//        template.setDefaultTopic("messages");
//        template.sendDefault(0, 0, "message1");
//        template.sendDefault(0, 1, "message2");
//        template.sendDefault(1, 2, "message3");
//        template.sendDefault(1, 3, "message4");
//        template.flush();
//        assertTrue(latch.await(20, TimeUnit.SECONDS));
//        container.stop();
//    }
//
//    @Test
//    public void testEmbeddedRawKafka() throws Exception {
//
//
//        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
//        KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
//        producer.send(new ProducerRecord<>("messages", 0, 0, "message0")).get();
//        producer.send(new ProducerRecord<>("messages", 0, 1, "message1")).get();
//        producer.send(new ProducerRecord<>("messages", 1, 2, "message2")).get();
//        producer.send(new ProducerRecord<>("messages", 1, 3, "message3")).get();
//
//
//        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
//        consumerProps.put("auto.offset.reset", "earliest");
//
//        final CountDownLatch latch = new CountDownLatch(4);
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        executorService.execute(() -> {
//            KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
//            kafkaConsumer.subscribe(Collections.singletonList("messages"));
//            try {
//                while (true) {
//                    ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);
//                    for (ConsumerRecord<Integer, String> record : records) {
//                        LOGGER.info("consuming from topic = {}, partition = {}, offset = {}, key = {}, value = {}",
//                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
//                        latch.countDown();
//                    }
//                }
//            } finally {
//            	producer.close();
//                kafkaConsumer.close();
//            }
//        });
//
//        assertTrue(latch.await(90, TimeUnit.SECONDS));
//    }
//
//	@After
//	public void teardown(){
//		kafkaServer.shutdown();
//		zkClient.close();
//		zkServer.shutdown();
//	}
//}