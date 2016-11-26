package datacrawler;

import java.util.List;

/**
 * Created by cy111966 on 2016/11/16.
 */
public class SdLtHolderPerTimeContain {
  private SdLtHolderPerTime singleNew;//单一股票最新一期

  private List<SdLtHolderPerTime> singleAll;//单一股票所有期

  public List<SdLtHolderPerTime> getSingleAll() {
    return singleAll;
  }

  public void setSingleAll(List<SdLtHolderPerTime> singleAll) {
    this.singleAll = singleAll;
  }

  public SdLtHolderPerTime getSingleNew() {
    return singleNew;
  }

  public void setSingleNew(SdLtHolderPerTime singleNew) {
    this.singleNew = singleNew;
  }
}
