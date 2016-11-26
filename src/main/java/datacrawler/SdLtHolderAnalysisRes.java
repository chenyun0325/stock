package datacrawler;

import java.util.Set;

/**
 * Created by cy111966 on 2016/11/17.
 */
public class SdLtHolderAnalysisRes {
   private String key1;
  private String key2;
  private Set<String> crossName;

  public Set<String> getCrossName() {
    return crossName;
  }

  public void setCrossName(Set<String> crossName) {
    this.crossName = crossName;
  }

  public String getKey1() {
    return key1;
  }

  public void setKey1(String key1) {
    this.key1 = key1;
  }

  public String getKey2() {
    return key2;
  }

  public void setKey2(String key2) {
    this.key2 = key2;
  }
}
