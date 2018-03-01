package adept.e2e.kbresolver;

import adept.kbapi.KBEntity;
import adept.kbapi.KBPredicateArgument;
import adept.kbapi.KBRelation;

/**
 * Created by bmin on 11/30/16.
 */
public class StringUtil {
  public static String get_debug_info(KBRelation kbRelation) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(kbRelation.getType().getURI() + ": ");
    stringBuilder.append("<");
    for(KBPredicateArgument kbPredicateArgument : kbRelation.getArguments()) {
      if(kbPredicateArgument instanceof KBEntity) {
        KBEntity kbEntity = (KBEntity) kbPredicateArgument;
        stringBuilder.append(kbEntity.getCanonicalString() + ", ");
      }
    }
    stringBuilder.append(">");

    return stringBuilder.toString();
  }

  public static String getNormalizedString(String text) {
    if(text ==null)
      return "NA";

    return text.replace("\r", " ").replace("\n", " ").replace("\t", " ").trim();
  }
}
