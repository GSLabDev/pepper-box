package com.gslab.pepper.config.plaintext;

import com.gslab.pepper.util.PropsKeys;
import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TextAreaEditor;
import java.beans.PropertyDescriptor;

/**
 * The PlainTextConfigElementBeanInfo is UI for PlainTextConfigElement class. This create TextArea where template is designed.
 *
 * @Author  Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 28/02/2017
 */
public class PlainTextConfigElementBeanInfo extends BeanInfoSupport {

    //PlainTextConfigElement class property
   private static final String JSON_SCHEMA = "jsonSchema";

    //Message placeholder key
    private static final String PLACE_HOLDER = "placeHolder";

   /**
    * Constructor which creates property group and creates UI for PlainTextConfigElement.
    */
    public PlainTextConfigElementBeanInfo() {

        super(PlainTextConfigElement.class);

        //Create property group
        createPropertyGroup("plain_text_load_generator", new String[] {
                PLACE_HOLDER, JSON_SCHEMA
        });

        PropertyDescriptor placeHolderProps = property( PLACE_HOLDER );
        placeHolderProps.setValue( NOT_UNDEFINED, Boolean.TRUE );
        placeHolderProps.setValue( DEFAULT, PropsKeys.MSG_PLACEHOLDER );
        placeHolderProps.setValue( NOT_EXPRESSION, Boolean.TRUE );

        //Create inout Text Area
        PropertyDescriptor p = property( JSON_SCHEMA );
        p.setPropertyEditorClass( TextAreaEditor.class );
        p.setValue( NOT_UNDEFINED, Boolean.TRUE );
    }
}
