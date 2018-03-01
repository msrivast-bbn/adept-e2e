import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

public class Version {
  public static void main(String[] args) {

    Properties prop = new Properties();
    try (InputStream in = Version.class.getResourceAsStream("git.properties")) {
      prop.load(in);
      Enumeration<?> e = prop.propertyNames();
      System.out.println("VERSION INFORMATION FOR A2KD JAR:");
      System.out.printf("------------------------------------------%n%n");
      while (e.hasMoreElements()) {
    	  String key = (String) e.nextElement();
    	  System.out.printf("%s -- %s%n", key, prop.getProperty(key));
      }
      System.out.printf("%n------------------------------------------%n");
    } catch (Exception e) {
      System.err.printf("Exception caught: %s%n", e);
      System.exit(1);
    }
  }
}
