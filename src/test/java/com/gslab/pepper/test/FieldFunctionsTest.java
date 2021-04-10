package com.gslab.pepper.test;

import com.gslab.pepper.input.FieldDataFunctions;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by satish on 5/3/17.
 */
public class FieldFunctionsTest {

    @Test
    public void verifyTimeFunctions() {

        assertTrue("Invalid timestamp value", FieldDataFunctions.TIMESTAMP() <= System.currentTimeMillis());
        assertTrue("Invalid timestamp between interval value", FieldDataFunctions.TIMESTAMP("01-05-1988 12:12:12", "01-05-2000 12:12:12") > 568363332000L);
        assertNotNull("Invalid date value", FieldDataFunctions.DATE("dd-MM-yyyy HH:mm:ss"));

    }

    @Test
    public void verifyRandomFunctions() {

        assertNotNull("Invalid random string value", FieldDataFunctions.RANDOM_STRING("1", "2"));
        assertTrue("Invalid int value", FieldDataFunctions.RANDOM_INT(1, 2) > 0);
        assertTrue("Invalid double value", FieldDataFunctions.RANDOM_DOUBLE(1.0, 2.0) > 0);
        assertTrue("Invalid float value", FieldDataFunctions.RANDOM_FLOAT(1.0F, 2.0F) > 0);
        assertTrue("Invalid long value", FieldDataFunctions.RANDOM_LONG(1, 2) > 0);
        assertEquals("Invalid random string", FieldDataFunctions.RANDOM_ALPHA_NUMERIC("A", 3), "AAA");


    }

    @Test
    public void verifyRandomRangeFunctions() {

        assertTrue("Invalid int range value", FieldDataFunctions.RANDOM_INT_RANGE(1, 3) <= 3);
        assertTrue("Invalid long range value", FieldDataFunctions.RANDOM_LONG_RANGE(1, 3) <= 3);
        assertTrue("Invalid float range value", FieldDataFunctions.RANDOM_FLOAT_RANGE(1.0F, 3.0F) <= 3);
        assertTrue("Invalid double range value", FieldDataFunctions.RANDOM_DOUBLE(1.0, 3.0) <= 3);

    }

    @Test
    public void verifyUserFunctions() {

        assertNotNull("Invalid first name value", FieldDataFunctions.FIRST_NAME());
        assertNotNull("Invalid last name value", FieldDataFunctions.LAST_NAME());
        assertNotNull("Invalid user name value", FieldDataFunctions.USERNAME());
        assertNotNull("Invalid email address value", FieldDataFunctions.EMAIL("test.com"));
        assertNotNull("Invalid Gender", FieldDataFunctions.GENDER());
        assertNotNull("Invalid PHONE number", FieldDataFunctions.PHONE());

    }

    @Test
    public void verifyUtilFunctions(){
        assertNotNull("Invalid UUID value", FieldDataFunctions.UUID());
        assertNotNull("Invalid IP4 Address", FieldDataFunctions.IPV4());
        assertNotNull("Invalid IPV6 Address", FieldDataFunctions.IPV6());
        boolean currentBoolean = FieldDataFunctions.BOOLEAN();
        assertTrue("Invalid boolean value", currentBoolean == true || currentBoolean == false);
//        assertEquals("Invalid sequence", FieldDataFunctions.SEQUENCE("randomSeq", 1, 1), 1);
    }
}
