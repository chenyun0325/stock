package datacrawler;

/**
 * Created by cy111966 on 2016/11/16.
 */
public class EastMoneyHolder {
  private String code;//股票代码
  private String codeName;//股票简称
  private String holderNum;//股东人数
  private String holderChange;//较上期变化
  private String numPerHolder;//人均流通股票数量
  private String ltSdStockNum;//十大流通_持股数量
  private String ltSdPer;//十大流通_占流通股比例
  private String sdStockNum;//十大_持股数量
  private String sdPer;//十大_占总股本比例
  private String jgStockNum;//机构持股数量
  private String jgPer;//机构_占流通股比例
  private String des;//筹码集中度
  private String x1;
  private String x2;
  private String date;//日期
  private String jc;//简称

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

  public String getDes() {
    return des;
  }

  public void setDes(String des) {
    this.des = des;
  }

  public String getHolderChange() {
    return holderChange;
  }

  public void setHolderChange(String holderChange) {
    this.holderChange = holderChange;
  }

  public String getHolderNum() {
    return holderNum;
  }

  public void setHolderNum(String holderNum) {
    this.holderNum = holderNum;
  }

  public String getJc() {
    return jc;
  }

  public void setJc(String jc) {
    this.jc = jc;
  }

  public String getJgPer() {
    return jgPer;
  }

  public void setJgPer(String jgPer) {
    this.jgPer = jgPer;
  }

  public String getJgStockNum() {
    return jgStockNum;
  }

  public void setJgStockNum(String jgStockNum) {
    this.jgStockNum = jgStockNum;
  }

  public String getLtSdPer() {
    return ltSdPer;
  }

  public void setLtSdPer(String ltSdPer) {
    this.ltSdPer = ltSdPer;
  }

  public String getLtSdStockNum() {
    return ltSdStockNum;
  }

  public void setLtSdStockNum(String ltSdStockNum) {
    this.ltSdStockNum = ltSdStockNum;
  }

  public String getNumPerHolder() {
    return numPerHolder;
  }

  public void setNumPerHolder(String numPerHolder) {
    this.numPerHolder = numPerHolder;
  }

  public String getSdPer() {
    return sdPer;
  }

  public void setSdPer(String sdPer) {
    this.sdPer = sdPer;
  }

  public String getSdStockNum() {
    return sdStockNum;
  }

  public void setSdStockNum(String sdStockNum) {
    this.sdStockNum = sdStockNum;
  }

  public String getX1() {
    return x1;
  }

  public void setX1(String x1) {
    this.x1 = x1;
  }

  public String getX2() {
    return x2;
  }

  public void setX2(String x2) {
    this.x2 = x2;
  }
}
