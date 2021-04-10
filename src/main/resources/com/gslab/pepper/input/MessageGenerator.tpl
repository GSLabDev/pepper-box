import java.util.Iterator;

import static com.gslab.pepper.input.FieldDataFunctions.*;
import static com.gslab.pepper.input.CustomFunctions.*;
import com.gslab.pepper.model.PlaintextMessage;

public class {{JAVA_CLASS_PLACEHOLDER}} implements Iterator<PlaintextMessage> {

 private static StringBuilder builder = new StringBuilder();

 @Override
 public boolean hasNext() {
     return true;
 }

 @Override
 public PlaintextMessage next() {
     builder.setLength(0);
     String headers = {{HEADER_PLACE_HOLDER}};
     builder.setLength(0);
     String payload = {{MESSAGE_PLACE_HOLDER}};
     return new PlaintextMessage(headers, payload);
 }

 @Override
 public void remove() {
 }
}