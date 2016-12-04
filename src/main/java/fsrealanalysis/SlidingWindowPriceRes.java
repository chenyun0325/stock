package fsrealanalysis;

/**
 * Created by cy111966 on 2016/12/4.
 */
public class SlidingWindowPriceRes {
  private long start;
  private String start_str;
  private long end;
  private String end_str;
  private double var_p;
  private double[] datas;

  public double[] getDatas() {
    return datas;
  }

  public void setDatas(double[] datas) {
    this.datas = datas;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public String getEnd_str() {
    return end_str;
  }

  public void setEnd_str(String end_str) {
    this.end_str = end_str;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public String getStart_str() {
    return start_str;
  }

  public void setStart_str(String start_str) {
    this.start_str = start_str;
  }

  public double getVar_p() {
    return var_p;
  }

  public void setVar_p(double var_p) {
    this.var_p = var_p;
  }
}
