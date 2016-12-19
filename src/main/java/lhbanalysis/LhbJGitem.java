package lhbanalysis;

import java.io.Serializable;

/**
 * Created by cy111966 on 2016/12/19.
 */
public class LhbJGitem implements Serializable {

  private String code;
  private String codeName;
  private String type;//买卖性质
  private String rank;//排名
  private String date;//时间
  private String jgname;//机构名称
  private String bmount;//买入金额
  private String smount;//卖出金额

  public String getBmount() {
    return bmount;
  }

  public void setBmount(String bmount) {
    this.bmount = bmount;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getCodeName() {
    return codeName;
  }

  public void setCodeName(String codeName) {
    this.codeName = codeName;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getJgname() {
    return jgname;
  }

  public void setJgname(String jgname) {
    this.jgname = jgname;
  }

  public String getRank() {
    return rank;
  }

  public void setRank(String rank) {
    this.rank = rank;
  }

  public String getSmount() {
    return smount;
  }

  public void setSmount(String smount) {
    this.smount = smount;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
