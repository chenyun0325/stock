package lhbanalysis;

import java.io.Serializable;

/**
 * Created by cy111966 on 2016/12/19.
 */
public class LhbItem implements Serializable{

  private String code;
  private String codeName;
  private String tradeDate;
  private String closePrice;
  private String zdf;
  private String comment;
  private String sign;

  public String getClosePrice() {
    return closePrice;
  }

  public void setClosePrice(String closePrice) {
    this.closePrice = closePrice;
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

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getSign() {
    return sign;
  }

  public void setSign(String sign) {
    this.sign = sign;
  }

  public String getTradeDate() {
    return tradeDate;
  }

  public void setTradeDate(String tradeDate) {
    this.tradeDate = tradeDate;
  }

  public String getZdf() {
    return zdf;
  }

  public void setZdf(String zdf) {
    this.zdf = zdf;
  }
}
