package datacrawler;

import java.util.List;

/**
 * Created by cy111966 on 2016/11/16.
 */
public class SdLtHolderPerTime {
  private String date;//日期
  private String code;//编码
  private String totalNum;//累计持股
  private String totalPer;//累计比例
  private String totalNumChang;//较上期变化
  private List<SdLtHolder> lists;

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public List<SdLtHolder> getLists() {
    return lists;
  }

  public void setLists(List<SdLtHolder> lists) {
    this.lists = lists;
  }

  public String getTotalNum() {
    return totalNum;
  }

  public void setTotalNum(String totalNum) {
    this.totalNum = totalNum;
  }

  public String getTotalNumChang() {
    return totalNumChang;
  }

  public void setTotalNumChang(String totalNumChang) {
    this.totalNumChang = totalNumChang;
  }

  public String getTotalPer() {
    return totalPer;
  }

  public void setTotalPer(String totalPer) {
    this.totalPer = totalPer;
  }
}
