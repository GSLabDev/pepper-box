package com.gslab.pepper.input;

import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.model.FieldExpressionMapping;
import com.gslab.pepper.util.PropsKeys;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.util.List;

/**
 * The SchemaParser class reads input schema/field expression mapping and converts it into series of java statements.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class SchemaParser {

    /**
     * Function gets schema template as input and translates it into series of java statements
     * @param inputSchema
     * @return
     * @throws IOException
     */
   public String getProcessedSchema(String inputSchema) throws PepperBoxException {

       try {
           PushbackReader pushbackReader = new PushbackReader(new StringReader(inputSchema), 80);

           int chr;

           StringBuilder token = new StringBuilder();

           StringBuilder processedSchema = new StringBuilder();

           processedSchema.append(PropsKeys.STR_BUILD_OBJ);

           while ((chr = pushbackReader.read()) != -1) {

               if (chr != PropsKeys.OPENING_BRACE) {

                   token.append((char) chr);

               } else if ((chr = pushbackReader.read()) != PropsKeys.OPENING_BRACE) {

                   pushbackReader.unread(chr);
                   token.append(PropsKeys.OPENING_BRACE);

               } else {

                   appendStaticString(token, processedSchema);
                   token.delete(0, token.length());
                   chr = pushbackReader.read();

                   while (chr != -1) {

                       if (chr != PropsKeys.CLOSING_BRACE) {

                           token.append((char) chr);
                           chr = pushbackReader.read();

                       } else if ((chr = pushbackReader.read()) != PropsKeys.CLOSING_BRACE) {

                           pushbackReader.unread((char) chr);
                           token.append(PropsKeys.CLOSING_BRACE);

                       } else {

                           processedSchema.append(PropsKeys.STR_APPEND);
                           processedSchema.append(token.toString().replaceAll(PropsKeys.ESC_QUOTE, PropsKeys.DOUBLE_ESC_QUOTE));
                           processedSchema.append(PropsKeys.CLOSING_BRACKET);
                           token.delete(0, token.length());
                           break;

                       }
                   }

               }
           }

           appendStaticString(token, processedSchema);
           processedSchema.append(PropsKeys.STR_TOSTRING);

           return processedSchema.toString().replaceAll(PropsKeys.STR_TAB, PropsKeys.ESCAPE_TAB).replaceAll(PropsKeys.STR_NEWLINE, PropsKeys.ESCAPE_NEWLINE).replaceAll(PropsKeys.STR_CARRIAGE_RETURN, PropsKeys.ESCAPE_CARRIAGE_RETURN);
       }catch (IOException e){
           throw new PepperBoxException(e);
       }
    }

    private void appendStaticString(StringBuilder token, StringBuilder processedSchema) {

        processedSchema.append(PropsKeys.STR_APPEND);
        processedSchema.append(PropsKeys.ESC_QUOTE);
        processedSchema.append(token.toString().replaceAll(PropsKeys.ESC_QUOTE, PropsKeys.TRIPLE_ESC_QUOTE));
        processedSchema.append(PropsKeys.ESC_QUOTE);
        processedSchema.append(PropsKeys.CLOSING_BRACKET);

    }

    /**
     * This function gets input fields and its expression mapping for a class and translates it into series of java statements
     * @param fieldExpressions
     * @return java statements
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public String getProcessedSchema(List<FieldExpressionMapping> fieldExpressions) throws PepperBoxException {

        try {
            StringBuilder builder = new StringBuilder();
            for (FieldExpressionMapping fieldExpression : fieldExpressions) {
                String expression = fieldExpression.getPropertyAsString(FieldExpressionMapping.FIELD_EXPRESSION);
                String fieldName = fieldExpression.getPropertyAsString(FieldExpressionMapping.FIELD_NAME);
                if (expression != null && !expression.equalsIgnoreCase(PropsKeys.IGNORE)) {
                    String methodName = new StringBuilder("serializedObj.set").append(StringUtils.capitalize(fieldName)).append("(").append(expression).append(");\n").toString();
                    builder.append(methodName);
                }
            }
            return builder.toString();
        }catch (Exception e) {
            throw new PepperBoxException(e);
        }
    }

}

