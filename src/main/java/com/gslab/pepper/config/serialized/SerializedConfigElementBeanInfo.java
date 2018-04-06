package com.gslab.pepper.config.serialized;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TableEditor;
import org.apache.jmeter.testbeans.gui.TypeEditor;

import com.gslab.pepper.input.serialized.ClassPropertyEditor;
import com.gslab.pepper.model.FieldExpressionMapping;
import com.gslab.pepper.util.PropsKeys;

/**
 * The SerializedConfigElementBeanInfo is UI for SerializedConfigElement class. This UI records class name and class field and expression mappings.
 *
 * @Author  Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 28/02/2017
 */
public class SerializedConfigElementBeanInfo extends BeanInfoSupport {

    //Class name property of SerializedConfigElement class
    private static final String CLASS_NAME = "className";

    //Field expression mapping of SerializedConfigElement class
    private static final String OBJ_PROPERTIES = "objProperties";

    //Message placeholder key
    private static final String PLACE_HOLDER = "placeHolder";

    /**
     * Constructor which creates property group and creates UI for SerializedConfigElement.
     */
    public SerializedConfigElementBeanInfo() {

        super(SerializedConfigElement.class);

        //Create Property group
        createPropertyGroup("serialized_load_generator", new String[] {
                PLACE_HOLDER, CLASS_NAME, OBJ_PROPERTIES
        });

        PropertyDescriptor placeHolderProps = property(PLACE_HOLDER);
        placeHolderProps.setValue(NOT_UNDEFINED, Boolean.TRUE);
        placeHolderProps.setValue(DEFAULT, PropsKeys.MSG_PLACEHOLDER);
        placeHolderProps.setValue(NOT_EXPRESSION, Boolean.TRUE);

        //Create table editor component of jmeter for class field and expression mapping
        TypeEditor tableEditor = TypeEditor.TableEditor;
        PropertyDescriptor tableProperties = property(OBJ_PROPERTIES, tableEditor);
        tableProperties.setValue(TableEditor.CLASSNAME, FieldExpressionMapping.class.getName());
        tableProperties.setValue(TableEditor.HEADERS, new String[]{ "Field Name", "Field Expression" } );
        tableProperties.setValue(TableEditor.OBJECT_PROPERTIES, new String[]{ FieldExpressionMapping.FIELD_NAME, FieldExpressionMapping.FIELD_EXPRESSION } );
        tableProperties.setValue(DEFAULT, new ArrayList<>());
        tableProperties.setValue(NOT_UNDEFINED, Boolean.TRUE);

        //Create class name input textfield
        PropertyDescriptor classNameProps = property(CLASS_NAME);
        classNameProps.setPropertyEditorClass(ClassPropertyEditor.class);
        classNameProps.setValue(NOT_UNDEFINED, Boolean.TRUE);
        classNameProps.setValue(DEFAULT, "<POJO class name>");

    }

}
