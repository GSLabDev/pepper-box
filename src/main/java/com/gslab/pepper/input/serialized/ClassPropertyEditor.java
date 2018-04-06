package com.gslab.pepper.input.serialized;

import com.gslab.pepper.PepperBoxLoadGenerator;
import com.gslab.pepper.model.FieldExpressionMapping;
import com.gslab.pepper.util.PropsKeys;
import org.apache.jmeter.gui.ClearGui;
import org.apache.jmeter.gui.GuiPackage;
import org.apache.jmeter.testbeans.gui.GenericTestBeanCustomizer;
import org.apache.jmeter.testbeans.gui.TableEditor;
import org.apache.jmeter.testbeans.gui.TestBeanGUI;
import org.apache.jmeter.testbeans.gui.TestBeanPropertyEditor;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The ClassPropertyEditor is custom UI component for SerializedConfigElement class. This UI records class name and populates fields in table.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class ClassPropertyEditor extends PropertyEditorSupport implements ActionListener, TestBeanPropertyEditor, ClearGui {

    private static Logger logger = Logger.getLogger(PepperBoxLoadGenerator.class.getName());
    //input class field
    private final JTextField textField = new JTextField();

    //class load button
    private final JButton loadClassBtn = new JButton("Load Class");

    //layout panel for button and input textfield
    private final JPanel panel = new JPanel();

    //Property descriptor of class property editor
    private PropertyDescriptor propertyDescriptor = null;

    public ClassPropertyEditor() {
        this.init();
    }

    //Initialize UI component and layout
    //This will accept class name and button will load class properties in table editor
    private final void init() {

        panel.setLayout(new BorderLayout());
        panel.add(textField);
        panel.add(loadClassBtn, BorderLayout.AFTER_LINE_ENDS);
        this.loadClassBtn.addActionListener(this);
    }

    //create instance with initial value
    public ClassPropertyEditor(Object source) {
        super(source);
        this.init();
        this.setValue(source);

    }

    public ClassPropertyEditor(PropertyDescriptor descriptor) {
        super(descriptor);
        this.setPropertyDescriptor(descriptor);
        this.init();
    }

    @Override
    public String getAsText() {
        return this.textField.getText();
    }

    @Override
    public Component getCustomEditor() {
        return this.panel;
    }

    @Override
    public void setAsText(String text){
        this.textField.setText(text);
        this.textField.setCaretPosition(0);
    }

    @Override
    public void setValue(Object value) {
        if (value != null) {
            this.textField.setText(value.toString());
            this.textField.setCaretPosition(0);
        } else {
            this.textField.setText("");
        }

    }

    @Override
    public Object getValue() {
        return this.textField.getText();
    }

    @Override
    public boolean supportsCustomEditor() {
        return true;
    }

    //Load class button action listener
    @Override
    public void actionPerformed(ActionEvent actionEvent) {

        //Get class from input text field
        String className = this.textField.getText();

        //Get list of class properties
        List<FieldExpressionMapping> attributeList = new ArrayList<>();
        try {

            //Load class and get fields using reflection
            Class<?> loadedClass = Class.forName(className);

            Field[] fields = loadedClass.getDeclaredFields();

            for (Field field : fields) {

                field.setAccessible(true);

                FieldExpressionMapping expressionMapping = new FieldExpressionMapping();
                expressionMapping.setFieldName(field.getName());
                expressionMapping.setFieldExpression(PropsKeys.IGNORE);

                attributeList.add(expressionMapping);
            }

            //Get current test GUI component
            TestBeanGUI testBeanGUI = (TestBeanGUI) GuiPackage.getInstance().getCurrentGui();
            Field customizer = TestBeanGUI.class.getDeclaredField(PropsKeys.CUSTOMIZER);
            customizer.setAccessible(true);


            //From TestBeanGUI retrieve Bean Customizer as it includes all editors like ClassPropertyEditor, TableEditor
            GenericTestBeanCustomizer testBeanCustomizer = (GenericTestBeanCustomizer) customizer.get(testBeanGUI);
            Field editors = GenericTestBeanCustomizer.class.getDeclaredField(PropsKeys.EDITORS);
            editors.setAccessible(true);

            //Retrieve TableEditor and set all fields with default values to it
            PropertyEditor[] propertyEditors = (PropertyEditor[]) editors.get(testBeanCustomizer);
            for (PropertyEditor propertyEditor : propertyEditors){
                if (propertyEditor instanceof TableEditor){
                    propertyEditor.setValue(attributeList);
                }
            }

        } catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException e) {
            JOptionPane.showMessageDialog(null, "Failed to load class properties : " + e.getMessage(), "ERROR: Failed to load class properties!" , JOptionPane.ERROR_MESSAGE);
            logger.log(Level.SEVERE, "Failed to load class properties", e);
        }

    }

    @Override
    public void setDescriptor(PropertyDescriptor propertyDescriptor) {
        this.setPropertyDescriptor(propertyDescriptor);
    }

    @Override
    public void clearGui() {
        this.textField.setText("");
    }

	public PropertyDescriptor getPropertyDescriptor() {
		return propertyDescriptor;
	}

	public void setPropertyDescriptor(PropertyDescriptor propertyDescriptor) {
		this.propertyDescriptor = propertyDescriptor;
	}
}
