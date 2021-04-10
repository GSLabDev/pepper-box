package com.gslab.pepper.test;

import com.gslab.pepper.model.FieldExpressionMapping;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class TestInputUtils {

    public static String testHeaders = "{\n" +
            "\t\"requestId\":{{UUID()}}\n" +
            "}";

    public static String testHeadersWithMessageKey = "{\n" +
            "\t\"requestId\":\"{{UUID()}}\",\n" +
            "\t\"MESSAGE_KEY\":\"my-key\"\n" +
            "}";

    public static String testSchema = "{\n" +
            "\t\"messageId\":{{SEQUENCE(\"messageId\", 1, 1)}},\n" +
            "\t\"messageBody\":\"{{RANDOM_ALPHA_NUMERIC(\"abcedefghijklmnopqrwxyzABCDEFGHIJKLMNOPQRWXYZ\", 100)}}\",\n" +
            "\t\"messageCategory\":\"{{RANDOM_STRING(\"Finance\", \"Insurance\", \"Healthcare\", \"Shares\")}}\",\n" +
            "\t\"messageStatus\":\"{{RANDOM_STRING(\"Accepted\",\"Pending\",\"Processing\",\"Rejected\")}}\",\n" +
            "\t\"messageTime\":{{TIMESTAMP()}}\n" +
            "}";

    public static String defectSchema = "{\n" +
            "\t\"messageId\":{{WRONG_FUNCTION(\"messageId\", 1, 1)}},\n" +
            "\t\"messageBody\":\"{{RANDOM_ALPHA_NUMERIC(\"abcedefghijklmnopqrwxyzABCDEFGHIJKLMNOPQRWXYZ\", 100)}}\",\n" +
            "\t\"messageCategory\":\"{{RANDOM_STRING(\"Finance\", \"Insurance\", \"Healthcare\", \"Shares\")}}\",\n" +
            "\t\"messageStatus\":\"{{RANDOM_STRING(\"Accepted\",\"Pending\",\"Processing\",\"Rejected\")}}\",\n" +
            "\t\"messageTime\":{{TIMESTAMP()}}\n" +
            "}";


    public static String producerProps = "bootstrap.servers=%s:%s\n" +
            "zookeeper.servers=%s:%d\n" +
            "kafka.topic.name=test\n" +
            "key.serializer=org.apache.kafka.common.serialization.StringSerializer\n" +
            "value.serializer=org.apache.kafka.common.serialization.StringSerializer\n" +
            "acks=0\n" +
            "send.buffer.bytes=131072\n" +
            "receive.buffer.bytes=32768\n" +
            "batch.size=16384\n" +
            "linger.ms=0\n" +
            "buffer.memory=33554432\n" +
            "compression.type=none\n" +
            "security.protocol=PLAINTEXT\n" +
            "kerberos.auth.enabled=NO\n" +
            "java.security.auth.login.config=<JAAS File Location>\n" +
            "java.security.krb5.conf=<krb5.conf location>\n" +
            "sasl.kerberos.service.name=<kerboros service name>";


    public static List<FieldExpressionMapping> getFieldExpressionMappings() {
        List<FieldExpressionMapping> fieldExpressionMappings = new ArrayList<>();
        FieldExpressionMapping fieldExpressionMapping = new FieldExpressionMapping();
        fieldExpressionMapping.setFieldName("messageId");
        fieldExpressionMapping.setFieldExpression("SEQUENCE(\"messageId\", 1, 1)");

        Assert.assertNotNull(fieldExpressionMapping.getFieldName());
        Assert.assertNotNull(fieldExpressionMapping.getFieldExpression());

        FieldExpressionMapping fieldExpressionMapping1 = new FieldExpressionMapping();
        fieldExpressionMapping1.setFieldName("messageBody");
        fieldExpressionMapping1.setFieldExpression("\"Test Message\"");

        FieldExpressionMapping fieldExpressionMapping2 = new FieldExpressionMapping();
        fieldExpressionMapping2.setFieldName("messageStatus");
        fieldExpressionMapping2.setFieldExpression("RANDOM_STRING(\"Accepted\",\"Pending\",\"Processing\",\"Rejected\")");

        FieldExpressionMapping fieldExpressionMapping3 = new FieldExpressionMapping();
        fieldExpressionMapping3.setFieldName("messageCategory");
        fieldExpressionMapping3.setFieldExpression("RANDOM_STRING(\"Finance\",\"Insurance\",\"Healthcare\",\"Shares\")");

        FieldExpressionMapping fieldExpressionMapping4 = new FieldExpressionMapping();
        fieldExpressionMapping4.setFieldName("messageTime");
        fieldExpressionMapping4.setFieldExpression("TIMESTAMP()");

        fieldExpressionMappings.add(fieldExpressionMapping);
        fieldExpressionMappings.add(fieldExpressionMapping1);
        fieldExpressionMappings.add(fieldExpressionMapping2);
        fieldExpressionMappings.add(fieldExpressionMapping3);
        fieldExpressionMappings.add(fieldExpressionMapping4);
        return fieldExpressionMappings;
    }

    public static List<FieldExpressionMapping> getWrongFieldExpressionMappings() {
        List<FieldExpressionMapping> fieldExpressionMappings = new ArrayList<>();
        FieldExpressionMapping fieldExpressionMapping = new FieldExpressionMapping();
        fieldExpressionMapping.setFieldName("messageId");
        fieldExpressionMapping.setFieldExpression("WRONG_FUNCTION()");
        fieldExpressionMappings.add(fieldExpressionMapping);
        return fieldExpressionMappings;
    }
}
