package com.gslab.pepper.util;

/**
 * The PropsKeys is property constant class.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class PropsKeys {

    public static final char OPENING_BRACE = '{';
    public static final char CLOSING_BRACE = '}';
    public static final String STR_BUILD_OBJ = "builder";
    public static final String ESC_QUOTE = "\"";
    public static final String DOUBLE_ESC_QUOTE = "\\\"";
    public static final String TRIPLE_ESC_QUOTE = "\\\\\"";
    public static final String STR_APPEND = ".append ( ";
    public static final String STR_TOSTRING = ".toString()";
    public static final String CLOSING_BRACKET = " ) ";
    public static final String STR_EMPTY = "";
    public static final String STR_TAB = "\t";
    public static final String STR_NEWLINE = "\n";
    public static final String STR_CARRIAGE_RETURN = "\r";
    public static final String ESCAPE_TAB = "\\\\t";
    public static final String ESCAPE_NEWLINE = "\\\\n";
    public static final String ESCAPE_CARRIAGE_RETURN = "\\\\r";
    public static final String MSG_GEN_PLC_HLDR = "{{MESSAGE_PLACE_HOLDER}}";
    public static final String HEADER_GEN_PLC_HLDR = "{{HEADER_PLACE_HOLDER}}";
    public static final String JAVA_CLS_PLC_HLDR = "{{JAVA_CLASS_PLACEHOLDER}}";
    public static final String JAVA_CLASS = "MessageIterator";
    public static final String JAVA_EXT = ".java";
    public static final String CLASS_EXT = ".class";
    public static final String SPLIT_BY_DOT = "\\.";
    public static final String MSG_PLACEHOLDER = "MESSAGE";
    public static final String PLAINTEXT_MESSAGE_GENERATOR_TPL = "MessageGenerator.tpl";
    public static final String SERIALIZED_MESSAGE_GENERATOR_TPL = "SerializedMessageGenerator.tpl";
    public static final String IGNORE = "Ignore";
    public static final String CUSTOMIZER = "customizer";
    public static final String EDITORS = "editors";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String BROKER_IDS_ZK_PATH = "/brokers/ids";
    public static final String MESSAGE_PLACEHOLDER_KEY = "message.placeholder.key";
    public static final String OBJ_CLASS = "{{OBJ_CLASS}}";
    public static final String SASL_MECHANISM_PLAIN = "PLAIN";
    public static final String SASL_JAAS_CONFIG_EXAMPLE = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<your_username>\"  password=\"<your_password>\";";
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String SASL_MECHANISM = "sasl.mechanism";
}
