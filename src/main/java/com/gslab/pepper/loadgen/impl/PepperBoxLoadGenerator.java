package com.gslab.pepper.loadgen.impl;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.google.common.util.concurrent.RateLimiter;
import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.input.SchemaProcessor;
import com.gslab.pepper.model.PlaintextMessage;
import com.gslab.pepper.util.HeaderUtils;
import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.utils.CommandLineUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The com.gslab.pepper.PepperBoxLoadGenerator standalone load generator.
 * This class takes arguments like throttle per thread, test duration, no of thread and schema file and kafka producer properties and generates load at throttled rate.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 28/02/2017
 */
public class PepperBoxLoadGenerator extends Thread {

    private static Logger LOGGER = Logger.getLogger(PepperBoxLoadGenerator.class.getName());
    private RateLimiter limiter;
    private Iterator iterator = null;
    private KafkaProducer<String, Object> producer;
    private String topic;
    private long durationInMillis;
    private HeaderUtils headerUtils = new HeaderUtils();

    /**
     * Start kafka load generator from input properties and schema
     *
     * @param schemaFile
     * @param producerProps
     * @param throughput
     * @param duration
     * @throws PepperBoxException
     */
    public PepperBoxLoadGenerator(String headersFile, String schemaFile, String producerProps, Integer throughput, Integer duration) throws PepperBoxException {

        Path schemPath = Paths.get(schemaFile);
        Path headersPath = Paths.get(headersFile);
        try {
            String inputSchema = new String(Files.readAllBytes(schemPath));
            String inputHeaders = new String(Files.readAllBytes(headersPath));
            SchemaProcessor schemaProcessor = new SchemaProcessor();
            iterator = schemaProcessor.getPlainTextMessageIterator(inputHeaders, inputSchema);
        } catch (IOException e) {
            throw new PepperBoxException(e);
        }
        Properties inputProps = new Properties();
        try {
            inputProps.load(new FileInputStream(producerProps));
        } catch (IOException e) {
            throw new PepperBoxException(e);
        }

        limiter = RateLimiter.create(throughput);
        durationInMillis = TimeUnit.SECONDS.toMillis(duration);
        Properties brokerProps = new Properties();
        brokerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, inputProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        brokerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, inputProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        brokerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, inputProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        brokerProps.put(ProducerConfig.ACKS_CONFIG, inputProps.getProperty(ProducerConfig.ACKS_CONFIG));
        brokerProps.put(ProducerConfig.SEND_BUFFER_CONFIG, inputProps.getProperty(ProducerConfig.SEND_BUFFER_CONFIG));
        brokerProps.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, inputProps.getProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG));
        brokerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, inputProps.getProperty(ProducerConfig.BATCH_SIZE_CONFIG));
        brokerProps.put(ProducerConfig.LINGER_MS_CONFIG, inputProps.getProperty(ProducerConfig.LINGER_MS_CONFIG));
        brokerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, inputProps.getProperty(ProducerConfig.BUFFER_MEMORY_CONFIG));
        brokerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, inputProps.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));

        String kerbsEnabled = inputProps.getProperty(ProducerKeys.KERBEROS_ENABLED);

        if (kerbsEnabled != null && kerbsEnabled.equals(ProducerKeys.FLAG_YES)) {

            brokerProps.put(PropsKeys.SASL_MECHANISM, inputProps.getProperty(PropsKeys.SASL_MECHANISM));
            brokerProps.put("security.protocol", inputProps.getProperty("security.protocol"));
            brokerProps.put(PropsKeys.SASL_JAAS_CONFIG, inputProps.getProperty(PropsKeys.SASL_JAAS_CONFIG));
            System.setProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, inputProps.getProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG));
            brokerProps.put(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, inputProps.getProperty(ProducerKeys.SASL_KERBEROS_SERVICE_NAME));
        }

        Set<String> parameters = inputProps.stringPropertyNames();
        parameters.forEach(parameter -> {
            if (parameter.startsWith("_")) {
                brokerProps.put(parameter.substring(1), inputProps.getProperty(parameter));
            }
        });

        topic = inputProps.getProperty(ProducerKeys.KAFKA_TOPIC_CONFIG);
        producer = new KafkaProducer<>(brokerProps);
    }

    /**
     * Retrieve brokers from zookeeper servers
     *
     * @param properties
     * @return
     */
    private String getBrokerServers(Properties properties) {

        StringBuilder kafkaBrokers = new StringBuilder();

        String zookeeperServers = properties.getProperty(ProducerKeys.ZOOKEEPER_SERVERS);

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

                LOGGER.log(Level.SEVERE, "Failed to get broker information", e);

            }

        }

        if (kafkaBrokers.length() > 0) {

            kafkaBrokers.setLength(kafkaBrokers.length() - 1);

            return kafkaBrokers.toString();

        } else {

            return properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        }
    }

    @Override
    public void run() {

        long endTime = durationInMillis + System.currentTimeMillis();
        while (endTime > System.currentTimeMillis()) {
            sendMessage();
        }
    }

    public void sendMessage() {
        limiter.acquire();
        PlaintextMessage message = (PlaintextMessage)iterator.next();
        try {
            LOGGER.log(Level.INFO, "Sending message: " + message);
            String messageKey = headerUtils.getMessageKey(message.getHeaders());
            Iterable<Header> headers = headerUtils.generateHeaders(message.getHeaders());

            ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(topic, null, messageKey, message.getPayload(), headers);
            RecordMetadata result = producer.send(producerRecord).get();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to send message", e);
        }
    }

    public static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec... required) {
        for (OptionSpec optionSpec : required) {
            if (!options.has(optionSpec)) {
                CommandLineUtils.printUsageAndDie(parser, "Missing required argument \"" + optionSpec + "\"");
            }
        }
    }

    public static void main(String[] args) {
        OptionParser parser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> schemaFile = parser.accepts("schema-file", "REQUIRED: Input schema file absolute path.")
                .withRequiredArg()
                .describedAs("schema file")
                .ofType(String.class);
        ArgumentAcceptingOptionSpec<String> headersFile = parser.accepts("headers-file", "REQUIRED: Input headers file absolute path.")
                .withRequiredArg()
                .describedAs("headers file")
                .ofType(String.class);
        ArgumentAcceptingOptionSpec<String> producerConfig = parser.accepts("producer-config-file", "REQUIRED: Kafka producer properties file absolute path.")
                .withRequiredArg()
                .describedAs("producer properties")
                .ofType(String.class);
        ArgumentAcceptingOptionSpec<Integer> throughput = parser.accepts("throughput-per-producer", "REQUIRED: Throttle rate per thread.")
                .withRequiredArg()
                .describedAs("throughput")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<Integer> duration = parser.accepts("test-duration", "REQUIRED: Test duration in seconds.")
                .withRequiredArg()
                .describedAs("test duration")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<Integer> threadCount = parser.accepts("num-producers", "REQUIRED: Number of producer threads.")
                .withRequiredArg()
                .describedAs("producers")
                .ofType(Integer.class);

        if (args.length == 0) {
            CommandLineUtils.printUsageAndDie(parser, "Kafka console load generator.");
        }
        OptionSet options = parser.parse(args);
        checkRequiredArgs(parser, options, schemaFile, producerConfig, throughput, duration, threadCount);
        try {
            int totalThreads = options.valueOf(threadCount);
            for (int i = 0; i < totalThreads; i++) {
                PepperBoxLoadGenerator jsonProducer = new PepperBoxLoadGenerator(options.valueOf(headersFile), options.valueOf(schemaFile), options.valueOf(producerConfig), options.valueOf(throughput), options.valueOf(duration));
                jsonProducer.start();
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to generate load", e);
        }
    }

}
