package com.gslab.pepper.test;

import java.beans.PropertyDescriptor;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.gslab.pepper.config.plaintext.PlainTextConfigElement;
import com.gslab.pepper.config.plaintext.PlainTextConfigElementBeanInfo;
import com.gslab.pepper.config.serialized.SerializedConfigElement;
import com.gslab.pepper.config.serialized.SerializedConfigElementBeanInfo;
import com.gslab.pepper.input.SchemaProcessor;
import com.gslab.pepper.input.serialized.ClassPropertyEditor;
import com.gslab.pepper.model.FieldExpressionMapping;
import com.gslab.pepper.util.PropsKeys;

/**
 * Created by satish on 26/2/17.
 */
public class PepperBoxConfigElementTest {

    @BeforeClass
    public static void setUp(){
        JMeterContext jmcx = JMeterContextService.getContext();
        jmcx.setVariables(new JMeterVariables());
    }

    @Test
    public void plainTextConfigTest(){

        PlainTextConfigElement plainTextConfigElement = new PlainTextConfigElement();
        plainTextConfigElement.setJsonSchema(TestInputUtils.testSchema);
        plainTextConfigElement.setPlaceHolder(PropsKeys.MSG_PLACEHOLDER);
        plainTextConfigElement.iterationStart(null);
        Object object = JMeterContextService.getContext().getVariables().getObject(PropsKeys.MSG_PLACEHOLDER);
        JSONObject jsonObject = new JSONObject(object.toString());
        Assert.assertTrue("Failed to run config element", (Integer)jsonObject.get("messageId") > 0);

    }

    @Test(expected = ClassFormatError.class)
    public void plainTextExceptionTest(){

        PlainTextConfigElement plainTextConfigElement = new PlainTextConfigElement();
        plainTextConfigElement.setJsonSchema(TestInputUtils.defectSchema);
        plainTextConfigElement.setPlaceHolder(PropsKeys.MSG_PLACEHOLDER);
        JMeterContextService.getContext().getVariables().remove(PropsKeys.MSG_PLACEHOLDER);
        plainTextConfigElement.iterationStart(null);
        Object object = JMeterContextService.getContext().getVariables().getObject(PropsKeys.MSG_PLACEHOLDER);
        Assert.assertNull("Failed to run config element", object);

    }

    @Test
    public void serializedConfigTest(){

        List<FieldExpressionMapping> fieldExpressionMappings = TestInputUtils.getFieldExpressionMappings();
        SerializedConfigElement serializedConfigElement = new SerializedConfigElement();
        serializedConfigElement.setClassName("com.gslab.pepper.test.Message");
        serializedConfigElement.setObjProperties(fieldExpressionMappings);
        serializedConfigElement.setPlaceHolder(PropsKeys.MSG_PLACEHOLDER);
        JMeterContextService.getContext().getVariables().remove(PropsKeys.MSG_PLACEHOLDER);
        serializedConfigElement.iterationStart(null);
        Message message = (Message)JMeterContextService.getContext().getVariables().getObject(PropsKeys.MSG_PLACEHOLDER);
        Assert.assertEquals("Failed to run config element","Test Message", message.getMessageBody());

    }

    public void serializedConfigErrorTest(){

        List<FieldExpressionMapping> fieldExpressionMappings = TestInputUtils.getWrongFieldExpressionMappings();
        SerializedConfigElement serializedConfigElement = new SerializedConfigElement();
        serializedConfigElement.setClassName("com.gslab.pepper.test.Message");
        serializedConfigElement.setObjProperties(fieldExpressionMappings);
        serializedConfigElement.setPlaceHolder(PropsKeys.MSG_PLACEHOLDER);
        JMeterContextService.getContext().getVariables().remove(PropsKeys.MSG_PLACEHOLDER);
        serializedConfigElement.iterationStart(null);
        Message message = (Message)JMeterContextService.getContext().getVariables().getObject(PropsKeys.MSG_PLACEHOLDER);
        Assert.assertNull("Failed to run config element", message);

    }

    @Test
    public void validateSchemaProcessor(){

        try {

            SchemaProcessor schemaProcessor = new SchemaProcessor();
            Assert.assertTrue("Failed to generate Iterator from input schema", schemaProcessor.getPlainTextMessageIterator(TestInputUtils.testSchema) instanceof Iterator);

        } catch (Exception e) {
            Assert.assertTrue("Failed to generate Iterator from input schema : " + e.getMessage(), false);
        }
    }


    @Test(expected = Exception.class)
    public void validateClassPropertyEditor(){
            ResourceBundle.getBundle(PlainTextConfigElement.class.getName());
            PlainTextConfigElementBeanInfo pbeanInfo = new PlainTextConfigElementBeanInfo();
            Assert.assertTrue("Failed to validate serialized property descriptors", pbeanInfo.getPropertyDescriptors().length == 3);

            ResourceBundle.getBundle(SerializedConfigElement.class.getName());
            SerializedConfigElementBeanInfo sbeanInfo = new SerializedConfigElementBeanInfo();
            Assert.assertTrue("Failed to validate serialized property descriptors", sbeanInfo.getPropertyDescriptors().length == 3);

            PropertyDescriptor propertyDescriptor = sbeanInfo.getPropertyDescriptors()[1];
            ClassPropertyEditor classPropertyEditor = new ClassPropertyEditor(propertyDescriptor);
            classPropertyEditor.setValue("com.gslab.pepper.test.Message");
            classPropertyEditor.actionPerformed(null);
            Assert.assertEquals("Failed to validate serialized property descriptors", "com.gslab.pepper.test.Message", classPropertyEditor.getValue());
    }

}
