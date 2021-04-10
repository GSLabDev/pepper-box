package com.gslab.pepper.test;

import com.gslab.pepper.input.CustomFunctions;
import com.gslab.pepper.input.FieldDataFunctions;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by armand.ballaci on 26/3/21.
 */
public class CustomFunctionsTest {

    @Test
    public void verifyCustomFunctions() {

        assertTrue("Wrong number of elements generated", CustomFunctions.UUID_ARRAY(5, true).split(",").length == 5);
        assertNotNull("Should always return a non null/empty string", CustomFunctions.UUID_ARRAY(5, false));

    }

}
