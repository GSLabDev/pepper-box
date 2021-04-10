package com.gslab.pepper.input;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The CustomFunctions allows users to write custom functions and then it can be used in template.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class CustomFunctions {

    //Random value generator
    private static SplittableRandom random = new SplittableRandom();

    public static String UUID_ARRAY(int length, boolean fixed) {

        int elementCount = fixed ? length : random.nextInt(length) + 1;
        List<String> l = new ArrayList<String>();
        for(int i = 0; i < elementCount; i++){
            l.add("\"" + UUID.randomUUID().toString()+ "\"");
        }
        return l.stream().collect(Collectors.joining(","));

    }

    public static void main(String[] args) {
        String r = CustomFunctions.UUID_ARRAY(5, false);
        System.out.println(r);
    }

}
