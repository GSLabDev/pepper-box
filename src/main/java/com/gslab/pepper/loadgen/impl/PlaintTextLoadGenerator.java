package com.gslab.pepper.loadgen.impl;

import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.loadgen.BaseLoadGenerator;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;


import javax.swing.*;
import java.util.Iterator;
import java.util.ArrayList;
 
/**
 * The PlaintTextLoadGenerator is custom load generator class gets invoked from iteratorStart of PlainTextConfigElement class
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */

public class PlaintTextLoadGenerator implements BaseLoadGenerator {

    private transient Iterator<String> messageIterator = null; 
    private static final Logger log = LoggingManager.getLoggerForClass();

    /**
     * PlaintTextLoadGenerator constructor which initializes message iterator using schemaProcessor
     * @param jsonSchema
     */
    public PlaintTextLoadGenerator(String jsonSchema) throws PepperBoxException {
		
        try {
			log.info( "going to process jsonSchema: " + jsonSchema );
			ArrayList messages = new ArrayList();
			messages.add(jsonSchema);
            this.messageIterator = messages.iterator();
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
        Object val = null;
        if ( messageIterator.hasNext() ){
           val = messageIterator.next();
        }
        return val;
    }
}
