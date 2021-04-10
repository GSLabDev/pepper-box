package com.gslab.pepper.input;

import com.gslab.pepper.PepperBoxLoadGenerator;
import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.input.compile.InMemoryJavaCompiler;
import com.gslab.pepper.util.PropsKeys;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The SchemaTranslator class gets series of java statements and generates iterator using class template
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class SchemaTranslator {

    private static Logger LOGGER = Logger.getLogger(PepperBoxLoadGenerator.class.getName());

    /**
     * Creates plain text iterator
     *
     * @param schemExecStatement
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Iterator getPlainTextMsgIterator(String headerExecStatement, String schemExecStatement) throws PepperBoxException {


        try {

            //Get file name without extension i.e. class name
            String classname =  PropsKeys.JAVA_CLASS + System.currentTimeMillis();

            //Replace placeholders in template and get java source code
            final String messageGeneratorSource = getTemplateFileContent(PropsKeys.PLAINTEXT_MESSAGE_GENERATOR_TPL).replace(PropsKeys.HEADER_GEN_PLC_HLDR, headerExecStatement).replace(PropsKeys.MSG_GEN_PLC_HLDR, schemExecStatement).replace(PropsKeys.JAVA_CLS_PLC_HLDR, classname);

            //Compile class from java source and load class in jvm
            Class<?> messageIterator = InMemoryJavaCompiler.compileSchemaClass(classname, messageGeneratorSource);

            //Get Iterator instance
            return (Iterator) messageIterator.newInstance();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to compileSchemaClass class", e);
            throw new PepperBoxException(e);
        }
    }


    /**
     * Creates serialized Object Iterator
     *
     * @param pojoClass
     * @param execStatement
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Iterator getSerializedMsgIterator(String pojoClass, String execStatement) throws PepperBoxException {

        try {

            //Get file name without extension i.e. class name
            String classname =  PropsKeys.JAVA_CLASS + System.currentTimeMillis();

            //Replace placeholders in template and get java source code
            final String messageGeneratorSource = getTemplateFileContent(PropsKeys.SERIALIZED_MESSAGE_GENERATOR_TPL).replace(PropsKeys.MSG_GEN_PLC_HLDR, execStatement).replace(PropsKeys.JAVA_CLS_PLC_HLDR, classname).replace(PropsKeys.OBJ_CLASS, pojoClass);

            //Compile class from java source and load class in jvm
            Class<?> messageIterator = InMemoryJavaCompiler.compileSchemaClass(classname, messageGeneratorSource);

            //Get Iterator instance
            return (Iterator) messageIterator.newInstance();


        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to compileSchemaClass class", e);
            throw new PepperBoxException(e);
        }
    }

    /**
     * Read template with given template name
     *
     * @param template
     * @return template string
     */
    private String getTemplateFileContent(String template) {
        StringBuilder builder = new StringBuilder();
        InputStream in = getClass().getResourceAsStream(template);
        Scanner scanner = new Scanner(in);
        while (scanner.hasNextLine()) {
            builder.append(scanner.nextLine());
        }
        return builder.toString();

    }


}