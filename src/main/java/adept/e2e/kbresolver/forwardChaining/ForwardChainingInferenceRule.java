package adept.e2e.kbresolver.forwardChaining;

import adept.e2e.kbresolver.forwardChaining.ForwardChainingInferenceHelper;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Created by bmin on 11/30/16.
 */
public class ForwardChainingInferenceRule {
  List<String> ruleInfo;

  ForwardChainingInferenceRule(ImmutableList<String> ruleInfo) {
    this.ruleInfo = ruleInfo;
  }

  public String getInferedArg1Role() {
    return ruleInfo.get(7);
  }

  public String getInferedArg2Role() {
    return ruleInfo.get(8);
  }

  public String getInferedRelationType() {
    return ruleInfo.get(6);
  }

  boolean isOneRelationToOneRelationRule() {
    if(ruleInfo.get(3).isEmpty() || ruleInfo.get(4).isEmpty() || ruleInfo.get(5).isEmpty())
      return true;
    else
      return false;
  }

  public String getQuery() {
    if(isOneRelationToOneRelationRule()) {
      String query = String.format(ForwardChainingInferenceHelper.queryTemplateOneRelationRule,
          ruleInfo.get(0), ruleInfo.get(1), ruleInfo.get(2));
      return query;
    }
    else {
      String query = String.format(ForwardChainingInferenceHelper.queryTemplateTwoRelationRule,
          ruleInfo.get(0), ruleInfo.get(1), ruleInfo.get(2),
          ruleInfo.get(3), ruleInfo.get(4), ruleInfo.get(5));
      return query;
    }
  }

  @Override
  public int hashCode() {
    int ret = 0;
    for(String r : ruleInfo)
      ret = ret * 31 + r.hashCode();

    return ret;
  }
}
