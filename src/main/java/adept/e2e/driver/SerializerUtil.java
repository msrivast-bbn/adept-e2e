package adept.e2e.driver;

import java.io.UnsupportedEncodingException;

import adept.serialization.XMLStringSerializer;

/**
 * Created by msrivast on 10/28/16.
 */
public final class SerializerUtil {

  private static XMLStringSerializer xmlStringSerializer = new XMLStringSerializer();

  public static <T> String serialize(T t) throws UnsupportedEncodingException {
    return xmlStringSerializer.serializeToString(t);
  }

  public static <T> T deserialize(String string, Class<T> className) throws
                                                                     UnsupportedEncodingException {
    return (T) xmlStringSerializer.deserializeFromString(string, className);
  }
}
