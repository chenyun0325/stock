package datacrawler;

/**
 * Created by cy111966 on 2016/11/16.
 */
public class SdLtHolder {
  private String org;//机构或者基金
  private String num;//数量
  private String change;//变化
  private String per;//比例
  private String esCost;//成本
  private String perChange;//变化比例
  private String type;//股份类型
  private String des;//详情

  public String getDes() {
    return des;
  }

  public void setDes(String des) {
    this.des = des;
  }

  public String getChange() {
    return change;
  }

  public void setChange(String change) {
    this.change = change;
  }

  public String getEsCost() {
    return esCost;
  }

  public void setEsCost(String esCost) {
    this.esCost = esCost;
  }

  public String getNum() {
    return num;
  }

  public void setNum(String num) {
    this.num = num;
  }

  public String getOrg() {
    return org;
  }

  public void setOrg(String org) {
    this.org = org;
  }

  public String getPer() {
    return per;
  }

  public void setPer(String per) {
    this.per = per;
  }

  public String getPerChange() {
    return perChange;
  }

  public void setPerChange(String perChange) {
    this.perChange = perChange;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
