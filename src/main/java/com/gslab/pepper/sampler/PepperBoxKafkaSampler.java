
package com.gslab.pepper.sampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gslab.pepper.model.PlaintextMessage;
import com.gslab.pepper.util.HeaderUtils;
import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.log.Logger;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * The PepperBoxKafkaSampler class custom java sampler for jmeter.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class PepperBoxKafkaSampler extends AbstractJavaSamplerClient {

    //kafka producer
    private KafkaProducer<String, Object> producer;

    // topic on which messages will be sent
    private String topic;

    //Message placeholder key
    private String placeHolder;

    private static final Logger log = LoggingManager.getLoggerForClass();

    /**
     * Set default parameters and their values
     *
     * @return
     */
    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerKeys.BOOTSTRAP_SERVERS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.ZOOKEEPER_SERVERS, ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, ProducerKeys.KAFKA_TOPIC_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerKeys.KEY_SERIALIZER_CLASS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerKeys.VALUE_SERIALIZER_CLASS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.COMPRESSION_TYPE_CONFIG, ProducerKeys.COMPRESSION_TYPE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BATCH_SIZE_CONFIG, ProducerKeys.BATCH_SIZE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.LINGER_MS_CONFIG, ProducerKeys.LINGER_MS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BUFFER_MEMORY_CONFIG, ProducerKeys.BUFFER_MEMORY_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.ACKS_CONFIG, ProducerKeys.ACKS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.SEND_BUFFER_CONFIG, ProducerKeys.SEND_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.RECEIVE_BUFFER_CONFIG, ProducerKeys.RECEIVE_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        defaultParameters.addArgument(PropsKeys.MESSAGE_PLACEHOLDER_KEY, PropsKeys.MSG_PLACEHOLDER);
        defaultParameters.addArgument(ProducerKeys.KERBEROS_ENABLED, ProducerKeys.KERBEROS_ENABLED_DEFULAT);
        defaultParameters.addArgument(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.JAVA_SEC_KRB5_CONFIG, ProducerKeys.JAVA_SEC_KRB5_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, ProducerKeys.SASL_KERBEROS_SERVICE_NAME_DEFAULT);
        defaultParameters.addArgument(PropsKeys.SASL_MECHANISM, PropsKeys.SASL_MECHANISM_PLAIN);
        defaultParameters.addArgument(PropsKeys.SASL_JAAS_CONFIG, PropsKeys.SASL_JAAS_CONFIG_EXAMPLE);
        return defaultParameters;
    }

    /**
     * Gets invoked exactly once  before thread starts
     *
     * @param context
     */
    @Override
    public void setupTest(JavaSamplerContext context) {

        log.info("In setup Test Pepperbox...");
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getParameter(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.ACKS_CONFIG, context.getParameter(ProducerConfig.ACKS_CONFIG));
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, context.getParameter(ProducerConfig.SEND_BUFFER_CONFIG));
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, context.getParameter(ProducerConfig.RECEIVE_BUFFER_CONFIG));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, context.getParameter(ProducerConfig.BATCH_SIZE_CONFIG));
        props.put(ProducerConfig.LINGER_MS_CONFIG, context.getParameter(ProducerConfig.LINGER_MS_CONFIG));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getParameter(ProducerConfig.BUFFER_MEMORY_CONFIG));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, context.getParameter(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, context.getParameter(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));

        Iterator<String> parameters = context.getParameterNamesIterator();
        parameters.forEachRemaining(parameter -> {
            if (parameter.startsWith("_")) {
                props.put(parameter.substring(1), context.getParameter(parameter));
            }
        });
        String kerberosEnabled = context.getParameter(ProducerKeys.KERBEROS_ENABLED);
        if (kerberosEnabled != null && kerberosEnabled.equals(ProducerKeys.FLAG_YES)) {
            props.put(PropsKeys.SASL_MECHANISM, context.getParameter(PropsKeys.SASL_MECHANISM));
            props.put(PropsKeys.SASL_JAAS_CONFIG, context.getParameter(PropsKeys.SASL_JAAS_CONFIG));
            System.setProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, context.getParameter(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG));
            props.put(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, context.getParameter(ProducerKeys.SASL_KERBEROS_SERVICE_NAME));
        }
        Enumeration keys = props.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            String value = (String) props.get(key);
            log.info(key + ": " + value);
        }

        placeHolder = context.getParameter(PropsKeys.MESSAGE_PLACEHOLDER_KEY);
        topic = context.getParameter(ProducerKeys.KAFKA_TOPIC_CONFIG);
        producer = new KafkaProducer<String, Object>(props);

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
        PlaintextMessage message = (PlaintextMessage) JMeterContextService.getContext().getVariables().getObject(placeHolder);
        HeaderUtils headerUtils = new HeaderUtils();

        try {
            log.debug("Sending message: " + message);
            String messageKey = headerUtils.getMessageKey(message.getHeaders());
            Iterable<Header> headers = headerUtils.generateHeaders(message.getHeaders());
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, null, messageKey, message.getPayload(), headers);

            producer.send(producerRecord);
            sampleResult.setResponseData(message.toString(), StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(true);
            sampleResult.sampleEnd();

        } catch (Exception e) {
            log.error("Failed to send message", e);
            sampleResult.setResponseData(e.getMessage(), StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(false);
            sampleResult.sampleEnd();

        }

        return sampleResult;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }

}
