package adept.e2e.driver;

import adept.kbapi.KB;
import adept.kbapi.KBConfigurationException;
import adept.kbapi.KBParameters;


public final class KBSingleton {

  private static volatile KB instance;

  public static KB getInstance() throws Exception {
    if (instance == null) {
      synchronized (KBSingleton.class) {
        if (instance == null) {
          KBParameters kbParameters = new KBParameters();
          instance = new KB(kbParameters);
        }
      }
    }
    return instance;
  }

  public static KB getInstance(KBParameters kbParameters) throws
                                                          KBConfigurationException{
    if (instance == null) {
      synchronized (KBSingleton.class) {
        if (instance == null) {
          instance = new KB(kbParameters);
        }
      }
    }
    return instance;
  }
  public static KB getInstance(KBParameters kbParameters, String schema) throws Exception {
    if (instance == null) {
      synchronized (KBSingleton.class) {
        if (instance == null) {
          instance = new KB(kbParameters, schema);
        }
      }
    }
    return instance;
  }


}

