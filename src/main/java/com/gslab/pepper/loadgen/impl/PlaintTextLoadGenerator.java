package com.gslab.pepper.loadgen.impl;

import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.input.SchemaProcessor;
import com.gslab.pepper.loadgen.BaseLoadGenerator;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import javax.swing.*;
import java.util.Iterator;
/**
 * The PlaintTextLoadGenerator is custom load generator class gets invoked from iteratorStart of PlainTextConfigElement class
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */

public class PlaintTextLoadGenerator implements BaseLoadGenerator {

    private transient Iterator<String> messageIterator = null;

    private transient SchemaProcessor schemaProcessor = new SchemaProcessor();

    private static final Logger log = LoggingManager.getLoggerForClass();

    /**
     * PlaintTextLoadGenerator constructor which initializes message iterator using schemaProcessor
     * @param jsonSchema
     */
    public PlaintTextLoadGenerator(String headers, String jsonSchema) throws PepperBoxException {

        try {
            this.messageIterator = schemaProcessor.getPlainTextMessageIterator(headers, jsonSchema);
        }catch (Exception exc){
            log.error("Please make sure that expressions functions are already defined and parameters are correctly passed.", exc);
            throw new PepperBoxException(exc);
        }
    }

    /**
     * sends next message to PlainTextConfigElement
     * @return
     */
    @Override
    public Object nextMessage() {
        return messageIterator.next();
    }
}
