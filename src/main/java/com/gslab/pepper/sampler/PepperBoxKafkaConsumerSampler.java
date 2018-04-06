package com.gslab.pepper.sampler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;

/**
 * The PepperBoxKafkaSampler class custom java sampler for jmeter.
 */
public class PepperBoxKafkaConsumerSampler extends AbstractJavaSamplerClient {

	private KafkaConsumer<String, String> consumer;

	private static final Logger log = LoggerFactory.getLogger(PepperBoxKafkaConsumerSampler.class);

	/**
	 * Set default parameters and their values
	 *
	 * @return
	 */
	@Override
	public Arguments getDefaultParameters() {
		Arguments defaultParameters = new Arguments();
		defaultParameters.addArgument(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerKeys.BOOTSTRAP_SERVERS_CONFIG_DEFAULT);
		defaultParameters.addArgument(ProducerKeys.ZOOKEEPER_SERVERS, ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT);
		defaultParameters.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, ProducerKeys.KAFKA_TOPIC_CONFIG_DEFAULT);
		defaultParameters.addArgument(ConsumerConfig.GROUP_ID_CONFIG, "kafkaGroup");
		defaultParameters.addArgument(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
		defaultParameters.addArgument(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ProducerKeys.KEY_DESERIALIZER_CLASS_CONFIG_DEFAULT);
		defaultParameters.addArgument(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProducerKeys.VALUE_DESERIALIZER_CLASS_CONFIG_DEFAULT);
		defaultParameters.addArgument(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		defaultParameters.addArgument(ConsumerConfig.SEND_BUFFER_CONFIG, ProducerKeys.SEND_BUFFER_CONFIG_DEFAULT);
		defaultParameters.addArgument(ConsumerConfig.RECEIVE_BUFFER_CONFIG, ProducerKeys.RECEIVE_BUFFER_CONFIG_DEFAULT);
		defaultParameters.addArgument(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
		defaultParameters.addArgument(ProducerKeys.KERBEROS_ENABLED, ProducerKeys.FLAG_NO);
		defaultParameters.addArgument(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG_DEFAULT);
		defaultParameters.addArgument(ProducerKeys.JAVA_SEC_KRB5_CONFIG, ProducerKeys.JAVA_SEC_KRB5_CONFIG_DEFAULT);
		defaultParameters.addArgument(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, ProducerKeys.SASL_KERBEROS_SERVICE_NAME_DEFAULT);
		defaultParameters.addArgument(ProducerKeys.SASL_MECHANISM, ProducerKeys.SASL_MECHANISM_DEFAULT);
		defaultParameters.addArgument(ProducerKeys.SSL_ENABLED, ProducerKeys.FLAG_NO);
		defaultParameters.addArgument(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "<Key Password>");
		defaultParameters.addArgument(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "<Keystore Location>");
		defaultParameters.addArgument(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "<Keystore Password>");
		defaultParameters.addArgument(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "<Truststore Location>");
		defaultParameters.addArgument(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "<Truststore Password>");
		return defaultParameters;
	}

	/**
	 * Gets invoked exactly once  before thread starts
	 *
	 * @param context
	 */
	@Override
	public void setupTest(JavaSamplerContext context) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerServers(context));
		props.put(ConsumerConfig.SEND_BUFFER_CONFIG, context.getParameter(ConsumerConfig.SEND_BUFFER_CONFIG));
		props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, context.getParameter(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, context.getParameter(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
		props.put(ProducerKeys.SASL_MECHANISM, context.getParameter(ProducerKeys.SASL_MECHANISM));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, context.getParameter(ConsumerConfig.GROUP_ID_CONFIG));
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, context.getParameter(ConsumerConfig.CLIENT_ID_CONFIG));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, context.getParameter(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, context.getParameter(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, context.getParameter(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

		Iterator<String> parameters = context.getParameterNamesIterator();
		parameters.forEachRemaining(parameter -> {
			if (parameter.startsWith("_")) {
				props.put(parameter.substring(1), context.getParameter(parameter));
			}
		});

		String sslEnabled = context.getParameter(ProducerKeys.SSL_ENABLED);
		if (sslEnabled != null && sslEnabled.equals(ProducerKeys.FLAG_YES)) {
			props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
			props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, context.getParameter(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
			props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
			props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, context.getParameter(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
			props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
		}
		String kerberosEnabled = context.getParameter(ProducerKeys.KERBEROS_ENABLED);
		if (kerberosEnabled != null && kerberosEnabled.equals(ProducerKeys.FLAG_YES)) {
			System.setProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, context.getParameter(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG));
			System.setProperty(ProducerKeys.JAVA_SEC_KRB5_CONFIG, context.getParameter(ProducerKeys.JAVA_SEC_KRB5_CONFIG));
			props.put(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, context.getParameter(ProducerKeys.SASL_KERBEROS_SERVICE_NAME));
		}
		String topic = context.getParameter(ProducerKeys.KAFKA_TOPIC_CONFIG);
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
	}

	/**
	 * For each sample request this method is invoked and will return success/failure result
	 *
	 * @param context
	 * @return
	 */
	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		SampleResult sampleResult = new SampleResult();
		sampleResult.sampleStart();
		try {
			//Consumer
			ConsumerRecords<String, String> records = consumer.poll(30000);
			log.info("records.count : "+records.count());
			sampleResult.setResponseData(""+records.count(), null);
			sampleResult.setSuccessful(true);
			sampleResult.sampleEnd();
		} catch (Exception e) {
			log.error("Failed to read message", e);
			sampleResult.setResponseData(e.getMessage(), StandardCharsets.UTF_8.name());
			sampleResult.setSuccessful(false);
			sampleResult.sampleEnd();
		}
		return sampleResult;
	}

	@Override
	public void teardownTest(JavaSamplerContext context) {
		consumer.close();
	}

	private String getBrokerServers(JavaSamplerContext context) {
		StringBuilder kafkaBrokers = new StringBuilder();
		String zookeeperServers = context.getParameter(ProducerKeys.ZOOKEEPER_SERVERS);
		if (zookeeperServers != null && !zookeeperServers.equalsIgnoreCase(ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT)) {
			try {
				ZooKeeper zk = new ZooKeeper(zookeeperServers, 10000, null);
				List<String> ids = zk.getChildren(PropsKeys.BROKER_IDS_ZK_PATH, false);
				for (String id : ids) {
					String brokerInfo = new String(zk.getData(PropsKeys.BROKER_IDS_ZK_PATH + "/" + id, false, null));
					JsonObject jsonObject = Json.parse(brokerInfo).asObject();
					String brokerHost = jsonObject.getString(PropsKeys.HOST, "");
					int brokerPort = jsonObject.getInt(PropsKeys.PORT, -1);
					if (!brokerHost.isEmpty() && brokerPort != -1) {
						kafkaBrokers.append(brokerHost);
						kafkaBrokers.append(":");
						kafkaBrokers.append(brokerPort);
						kafkaBrokers.append(",");
					}
				}
			} catch (IOException | KeeperException | InterruptedException e) {
				log.error("Failed to get broker information", e);
			}
		}
		if (kafkaBrokers.length() > 0) {
			kafkaBrokers.setLength(kafkaBrokers.length() - 1);
			return kafkaBrokers.toString();
		} else {
			return  context.getParameter(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		}
	}
}