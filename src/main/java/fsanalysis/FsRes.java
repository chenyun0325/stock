package fsanalysis;

/**
 * Created by cy111966 on 2016/11/25.
 */
public class FsRes {
  private String code;//股票代码
  private String begin;//开始时间
  private long begin_l;
  private String end;//结束时间
  private long end_l;
  private long buy;//买量
  private long sale;//卖量
  private long diff_v;//买卖差
  private long amount_b;//买入金额
  private long amount_s;//卖出金额
  private long amount_diff;//买卖金额差
  private long amount_var;//增量资金

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public long getBegin_l() {
    return begin_l;
  }

  public void setBegin_l(long begin_l) {
    this.begin_l = begin_l;
  }

  public long getEnd_l() {
    return end_l;
  }

  public void setEnd_l(long end_l) {
    this.end_l = end_l;
  }

  public long getAmount_b() {
    return amount_b;
  }

  public void setAmount_b(long amount_b) {
    this.amount_b = amount_b;
  }

  public long getAmount_diff() {
    return amount_diff;
  }

  public void setAmount_diff(long amount_diff) {
    this.amount_diff = amount_diff;
  }

  public long getAmount_s() {
    return amount_s;
  }

  public void setAmount_s(long amount_s) {
    this.amount_s = amount_s;
  }

  public long getAmount_var() {
    return amount_var;
  }

  public void setAmount_var(long amount_var) {
    this.amount_var = amount_var;
  }

  public String getBegin() {
    return begin;
  }

  public void setBegin(String begin) {
    this.begin = begin;
  }

  public long getBuy() {
    return buy;
  }

  public void setBuy(long buy) {
    this.buy = buy;
  }

  public long getDiff_v() {
    return diff_v;
  }

  public void setDiff_v(long diff_v) {
    this.diff_v = diff_v;
  }

  public String getEnd() {
    return end;
  }

  public void setEnd(String end) {
    this.end = end;
  }

  public long getSale() {
    return sale;
  }

  public void setSale(long sale) {
    this.sale = sale;
  }

  @Override
  public String toString() {
    return code+","+begin+","+end+","+buy+","+sale+","+diff_v+","+amount_b+","+amount_s+","+amount_diff+","+amount_var;
  }
}
