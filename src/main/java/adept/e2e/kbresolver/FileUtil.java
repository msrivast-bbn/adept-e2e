package adept.e2e.kbresolver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bmin on 12/13/16.
 */
public class FileUtil {
  public static List<String> readLinesIntoList(String file) {
    List<String> lines = new ArrayList<String>();

    int nLine = 0;

    try {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String sline;
      while ((sline = reader.readLine()) != null) {
        if (nLine++ % 100000 == 0) {
          System.out.println("# lines read: " + nLine);
        }

        lines.add(sline);
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return lines;
  }

}
