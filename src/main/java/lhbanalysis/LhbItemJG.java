package lhbanalysis;

import java.io.Serializable;
import java.util.List;

/**
 * Created by cy111966 on 2016/12/19.
 */
public class LhbItemJG implements Serializable {
  private LhbItem item;//一条记录
  private List<LhbJGitem> jgList;//机构列表

  public LhbItem getItem() {
    return item;
  }

  public void setItem(LhbItem item) {
    this.item = item;
  }

  public List<LhbJGitem> getJgList() {
    return jgList;
  }

  public void setJgList(List<LhbJGitem> jgList) {
    this.jgList = jgList;
  }
}
