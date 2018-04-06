package com.gslab.pepper.input;


import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.model.FieldExpressionMapping;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * The SchemaProcessor class reads input schema/field expression mapping and returns iterator.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class SchemaProcessor {

    private SchemaParser schemaParser = new SchemaParser();

    private SchemaTranslator schemaTranslator = new SchemaTranslator();

    /**
     * Creates Iterator for plaintext config element with input schema
     * @param inputSchema
     * @return
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    public Iterator<String> getPlainTextMessageIterator(String inputSchema) throws PepperBoxException {

        String processedSchema = schemaParser.getProcessedSchema(inputSchema);
        return  schemaTranslator.getPlainTextMsgIterator(processedSchema);
    }

    /**
     * Creates Iterator for serialized config element
     * @param inputClass
     * @param fieldExpressions
     * @return
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    public Iterator<String> getSerializedMessageIterator(String inputClass, List<FieldExpressionMapping> fieldExpressions) throws PepperBoxException {

        String execStatements = schemaParser.getProcessedSchema(fieldExpressions);
        return  schemaTranslator.getSerializedMsgIterator(inputClass, execStatements);
    }
}