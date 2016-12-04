package fsrealanalysis;


import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import java.io.Serializable;

import fsanalysis.DateUtil;

/**
 * Created by cy111966 on 2016/12/3.
 */
public class SingleFsDataProcess implements FsDataProcess ,Serializable{

  StandardDeviation std = new StandardDeviation();

  public FsIndexRes process(FsData input) {
    String time_stamp = input.getDate()+" "+input.getTime();
    long time_stamp_long = DateUtil.convert2long(time_stamp, DateUtil.TIME_FORMAT);
    long cur_time_stamp = System.currentTimeMillis();
    double kp_price_dif = (input.getOpen() - input.getPre_close()) / input.getPre_close();
    double price_dif = (input.getPrice() - input.getPre_close()) / input.getPre_close();
    double yy_dif = (input.getAsk() - input.getBid())/input.getPrice();
    double b1_m = input.getB1_v() * input.getB1_p();
    double b2_m = input.getB2_v() * input.getB2_p();
    double b3_m = input.getB3_v() * input.getB3_p();
    double b4_m = input.getB4_v() * input.getB4_p();
    double b5_m = input.getB5_v() * input.getB5_p();
    double b_all_m=b1_m+b2_m+b3_m+b4_m+b5_m;
    // TODO: 2016/12/4 先不处理0的情况
    //数据归一化处理
    double b1_m_p = b1_m/b_all_m;
    double b2_m_p = b2_m/b_all_m;
    double b3_m_p = b3_m/b_all_m;
    double b4_m_p = b4_m/b_all_m;
    double b5_m_p = b5_m/b_all_m;
    double[] b_array ={b1_m_p,b2_m_p,b3_m_p,b4_m_p,b5_m_p};
    double b_var = std.evaluate(b_array);
    double a1_m = input.getA1_v() * input.getA1_p();
    double a2_m = input.getA2_v() * input.getA2_p();
    double a3_m = input.getA3_v() * input.getA3_p();
    double a4_m = input.getA4_v() * input.getA4_p();
    double a5_m = input.getA5_v() * input.getA5_p();
    double a_all_m=a1_m+a2_m+a3_m+a4_m+a5_m;
    double a1_m_p = a1_m/a_all_m;
    double a2_m_p = a2_m/a_all_m;
    double a3_m_p = a3_m/a_all_m;
    double a4_m_p = a4_m/a_all_m;
    double a5_m_p = a5_m/a_all_m;
    double[] a_array ={a1_m_p,a2_m_p,a3_m_p,a4_m_p,a5_m_p};
    double a_var = std.evaluate(a_array);
    double per = b_all_m / a_all_m;
    boolean b_ge_s = false;
    if (per > 1) {
      b_ge_s = true;
    }else {
      per=1/per;
    }
    double jd_per = b1_m / a1_m;
    boolean jd_b_ge_s = false;
    if (jd_per > 1) {
      jd_b_ge_s = true;
    }else {
      jd_per=1/jd_per;
    }
    double a1_p = a1_m / a_all_m;
    double b1_p = b1_m / b_all_m;
    input.setTimestamp(time_stamp_long);
    FsIndexRes indexRes = new FsIndexRes();
    indexRes.setFsData(input);
    indexRes.setTime_stamp(time_stamp);
    indexRes.setTime_stamp_long(time_stamp_long);
    indexRes.setKp_price_dif(kp_price_dif);
    indexRes.setPrice_dif(price_dif);
    indexRes.setYy_dif(yy_dif);
    indexRes.setB1_m(b1_m);
    indexRes.setB2_m(b2_m);
    indexRes.setB3_m(b3_m);
    indexRes.setB4_m(b5_m);
    indexRes.setB5_m(b5_m);
    indexRes.setB_all_m(b_all_m);
    indexRes.setA1_m(a1_m);
    indexRes.setA2_m(a2_m);
    indexRes.setA3_m(a3_m);
    indexRes.setA4_m(a4_m);
    indexRes.setA5_m(a5_m);
    indexRes.setA_all_m(a_all_m);
    indexRes.setPer(per);
    indexRes.setB_ge_s(b_ge_s);
    indexRes.setJd_per(jd_per);
    indexRes.setJd_b_ge_s(jd_b_ge_s);
    indexRes.setA1_p(a1_p);
    indexRes.setB1_p(b1_p);
    indexRes.setCurr_time_stamp(cur_time_stamp);
    indexRes.setA_var(a_var);
    indexRes.setB_var(b_var);
    return indexRes;
  }
}
