import java.util.Iterator;

import static com.gslab.pepper.input.FieldDataFunctions.*;
import static com.gslab.pepper.input.CustomFunctions.*;

public class {{JAVA_CLASS_PLACEHOLDER}} implements Iterator<String> {

 private static StringBuilder builder = new StringBuilder(18);

 @Override
 public boolean hasNext() {
     return true;
 }

 @Override
 public String next() {
     builder.setLength(0);
     return {{MESSAGE_PLACE_HOLDER}};
 }

 @Override
 public void remove() {
 }
}