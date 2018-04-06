package com.gslab.pepper.input.serialized;

import org.apache.kafka.common.serialization.Serializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The ObjectSerializer is custom Object serializer for kafka producer. This class takes object as input and returns byte array.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class ObjectSerializer implements Serializer<Object> {

    private static Logger logger = Logger.getLogger(ObjectSerializer.class.getName());
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        //TODO
    }

    @Override
    public byte[] serialize(String s, Object o) {

        byte[] retVal = null;

        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
        ) {

            out.writeObject(o);
            out.flush();
            retVal = bos.toByteArray();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to serialize object", e);
        }
        return retVal;
    }

    @Override
    public void close() {
        //TODO
    }
}
