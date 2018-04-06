package com.gslab.pepper.model;

import org.apache.jmeter.testelement.AbstractTestElement;

/**
 * The FieldExpressionMapping class represents class fields and associated expression mapping.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class FieldExpressionMapping extends AbstractTestElement {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3071719822227707332L;

	//Class field name property
    public static final String FIELD_NAME = "fieldName";

    //class field expression property
    public static final String FIELD_EXPRESSION = "fieldExpression";


    public String getFieldName() {
        return getProperty(FIELD_NAME).getStringValue();
    }

    public void setFieldName(String fieldName) {

        setProperty(FIELD_NAME, fieldName);

    }

    public String getFieldExpression() {

        return getProperty(FIELD_EXPRESSION).getStringValue();

    }

    public void setFieldExpression(String propertyValue) {

        setProperty(FIELD_EXPRESSION, propertyValue);

    }

}
