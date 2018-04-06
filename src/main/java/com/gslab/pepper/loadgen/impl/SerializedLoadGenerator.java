package com.gslab.pepper.loadgen.impl;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.input.SchemaProcessor;
import com.gslab.pepper.loadgen.BaseLoadGenerator;
import com.gslab.pepper.model.FieldExpressionMapping;
/**
 * The SerializedLoadGenerator is custom load generator class gets invoked from iteratorStart of SerializedConfigElement class
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class SerializedLoadGenerator implements BaseLoadGenerator {

    /**
	 * 
	 */
	private static final long serialVersionUID = 7427309255608917237L;

	private transient Iterator<String> messageIterator = null;

    private transient SchemaProcessor schemaProcessor = new SchemaProcessor();

	private static final Logger log = LoggerFactory.getLogger(SerializedLoadGenerator.class);

    /**
     * SerializedLoadGenerator constructor which initializes message iterator using schemaProcessor.
     * @param inputClass
     * @param fieldExprMappings
     */
    public SerializedLoadGenerator(String inputClass, List<FieldExpressionMapping> fieldExprMappings) throws PepperBoxException {

        try {

            this.messageIterator = (Iterator<String>) schemaProcessor.getSerializedMessageIterator(inputClass, fieldExprMappings);

        }catch (Exception exc){
            log.error("Please make sure that properties data type and expression function return type are compatible with each other", exc);
            throw new PepperBoxException(exc);
        }
    }

    @Override
    public Object nextMessage() {
        return this.messageIterator.next();
    }
}
