package fsrealanalysis;

/**
 * Created by cy111966 on 2016/12/1.
 * 夹单/托单/压单 等模型的分析结果
 */
public class FsIndexRes {
  private FsData fsData;//原始数据
  String time_stamp ;
  long time_stamp_long;
  long curr_time_stamp;
  double kp_price_dif ;//开盘涨跌幅度
  double price_dif;//价格涨跌幅度
  double yy_dif ;//卖出意愿,数值越大越不想以低价卖出
  double b1_m ;
  double b2_m;
  double b3_m;
  double b4_m;
  double b5_m ;
  double b_all_m;
  double a1_m;
  double a2_m ;
  double a3_m ;
  double a4_m;
  double a5_m;
  double a_all_m;
  double per ;//5档--买金额大于卖金额
  boolean b_ge_s;//买>卖
  double jd_per;
  boolean jd_b_ge_s;
  double a1_p ;//
  double b1_p;
  double a_var;//
  double b_var;//买方五档金额波动

  public double getA_var() {
    return a_var;
  }

  public void setA_var(double a_var) {
    this.a_var = a_var;
  }

  public double getB_var() {
    return b_var;
  }

  public void setB_var(double b_var) {
    this.b_var = b_var;
  }

  public long getCurr_time_stamp() {
    return curr_time_stamp;
  }

  public void setCurr_time_stamp(long curr_time_stamp) {
    this.curr_time_stamp = curr_time_stamp;
  }

  //夹单条件:a1_p>0.9&b1_p>0.9&jd_per=1
  public double getA1_m() {
    return a1_m;
  }

  public void setA1_m(double a1_m) {
    this.a1_m = a1_m;
  }

  public double getA1_p() {
    return a1_p;
  }

  public void setA1_p(double a1_p) {
    this.a1_p = a1_p;
  }

  public double getA2_m() {
    return a2_m;
  }

  public void setA2_m(double a2_m) {
    this.a2_m = a2_m;
  }

  public double getA3_m() {
    return a3_m;
  }

  public void setA3_m(double a3_m) {
    this.a3_m = a3_m;
  }

  public double getA4_m() {
    return a4_m;
  }

  public void setA4_m(double a4_m) {
    this.a4_m = a4_m;
  }

  public double getA5_m() {
    return a5_m;
  }

  public void setA5_m(double a5_m) {
    this.a5_m = a5_m;
  }

  public double getA_all_m() {
    return a_all_m;
  }

  public void setA_all_m(double a_all_m) {
    this.a_all_m = a_all_m;
  }

  public double getB1_m() {
    return b1_m;
  }

  public void setB1_m(double b1_m) {
    this.b1_m = b1_m;
  }

  public double getB1_p() {
    return b1_p;
  }

  public void setB1_p(double b1_p) {
    this.b1_p = b1_p;
  }

  public double getB2_m() {
    return b2_m;
  }

  public void setB2_m(double b2_m) {
    this.b2_m = b2_m;
  }

  public double getB3_m() {
    return b3_m;
  }

  public void setB3_m(double b3_m) {
    this.b3_m = b3_m;
  }

  public double getB4_m() {
    return b4_m;
  }

  public void setB4_m(double b4_m) {
    this.b4_m = b4_m;
  }

  public double getB5_m() {
    return b5_m;
  }

  public void setB5_m(double b5_m) {
    this.b5_m = b5_m;
  }

  public double getB_all_m() {
    return b_all_m;
  }

  public void setB_all_m(double b_all_m) {
    this.b_all_m = b_all_m;
  }

  public boolean isB_ge_s() {
    return b_ge_s;
  }

  public void setB_ge_s(boolean b_ge_s) {
    this.b_ge_s = b_ge_s;
  }

  public FsData getFsData() {
    return fsData;
  }

  public void setFsData(FsData fsData) {
    this.fsData = fsData;
  }

  public boolean isJd_b_ge_s() {
    return jd_b_ge_s;
  }

  public void setJd_b_ge_s(boolean jd_b_ge_s) {
    this.jd_b_ge_s = jd_b_ge_s;
  }

  public double getJd_per() {
    return jd_per;
  }

  public void setJd_per(double jd_per) {
    this.jd_per = jd_per;
  }

  public double getKp_price_dif() {
    return kp_price_dif;
  }

  public void setKp_price_dif(double kp_price_dif) {
    this.kp_price_dif = kp_price_dif;
  }

  public double getPer() {
    return per;
  }

  public void setPer(double per) {
    this.per = per;
  }

  public double getPrice_dif() {
    return price_dif;
  }

  public void setPrice_dif(double price_dif) {
    this.price_dif = price_dif;
  }

  public String getTime_stamp() {
    return time_stamp;
  }

  public void setTime_stamp(String time_stamp) {
    this.time_stamp = time_stamp;
  }

  public long getTime_stamp_long() {
    return time_stamp_long;
  }

  public void setTime_stamp_long(long time_stamp_long) {
    this.time_stamp_long = time_stamp_long;
  }

  public double getYy_dif() {
    return yy_dif;
  }

  public void setYy_dif(double yy_dif) {
    this.yy_dif = yy_dif;
  }
}
