package com.gslab.pepper.input.serialized;

import org.apache.kafka.common.serialization.Deserializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The ObjectDeserializer is custom Object deserializer for kafka consumer. This class takes byte array as input and returns object.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class ObjectDeserializer implements Deserializer<Object> {

    private static Logger logger = Logger.getLogger(ObjectDeserializer.class.getName());

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

    @Override
    public Object deserialize(String s, byte[] bytes) {
        Object receivedObj = null;

        try (
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(byteArrayInputStream);
        ) {
            receivedObj = ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            logger.log(Level.SEVERE, "Failed to deserialize object", e);
        }
        return receivedObj;
    }

    @Override
    public void close() {
        //TODO
    }


}
