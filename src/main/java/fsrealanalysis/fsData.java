package fsrealanalysis;

/**
 * Created by cy111966 on 2016/12/1.
 * 分时原始数据
 */
public class FsData {
  private long timestamp;//排序使用
  private String date;
  private String time;
  private String name;
  private double open;
  private double pre_close;
  private double price;
  private double high;
  private double low;
  private double bid;
  private double ask;
  private long volume;
  private double amount;
  private int b1_v;
  private double b1_p;
  private int b2_v;
  private double b2_p;
  private int b3_v;
  private double b3_p;
  private int b4_v;
  private double b4_p;
  private int b5_v;
  private double b5_p;
  private int a1_v;
  private double a1_p;
  private int a2_v;
  private double a2_p;
  private int a3_v;
  private double a3_p;
  private int a4_v;
  private double a4_p;
  private int a5_v;
  private double a5_p;

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public double getA1_p() {
    return a1_p;
  }

  public void setA1_p(double a1_p) {
    this.a1_p = a1_p;
  }

  public int getA1_v() {
    return a1_v;
  }

  public void setA1_v(int a1_v) {
    this.a1_v = a1_v;
  }

  public double getA2_p() {
    return a2_p;
  }

  public void setA2_p(double a2_p) {
    this.a2_p = a2_p;
  }

  public int getA2_v() {
    return a2_v;
  }

  public void setA2_v(int a2_v) {
    this.a2_v = a2_v;
  }

  public double getA3_p() {
    return a3_p;
  }

  public void setA3_p(double a3_p) {
    this.a3_p = a3_p;
  }

  public int getA3_v() {
    return a3_v;
  }

  public void setA3_v(int a3_v) {
    this.a3_v = a3_v;
  }

  public double getA4_p() {
    return a4_p;
  }

  public void setA4_p(double a4_p) {
    this.a4_p = a4_p;
  }

  public int getA4_v() {
    return a4_v;
  }

  public void setA4_v(int a4_v) {
    this.a4_v = a4_v;
  }

  public double getA5_p() {
    return a5_p;
  }

  public void setA5_p(double a5_p) {
    this.a5_p = a5_p;
  }

  public int getA5_v() {
    return a5_v;
  }

  public void setA5_v(int a5_v) {
    this.a5_v = a5_v;
  }

  public double getAmount() {
    return amount;
  }

  public void setAmount(double amount) {
    this.amount = amount;
  }

  public double getAsk() {
    return ask;
  }

  public void setAsk(double ask) {
    this.ask = ask;
  }

  public double getB1_p() {
    return b1_p;
  }

  public void setB1_p(double b1_p) {
    this.b1_p = b1_p;
  }

  public int getB1_v() {
    return b1_v;
  }

  public void setB1_v(int b1_v) {
    this.b1_v = b1_v;
  }

  public double getB2_p() {
    return b2_p;
  }

  public void setB2_p(double b2_p) {
    this.b2_p = b2_p;
  }

  public int getB2_v() {
    return b2_v;
  }

  public void setB2_v(int b2_v) {
    this.b2_v = b2_v;
  }

  public double getB3_p() {
    return b3_p;
  }

  public void setB3_p(double b3_p) {
    this.b3_p = b3_p;
  }

  public int getB3_v() {
    return b3_v;
  }

  public void setB3_v(int b3_v) {
    this.b3_v = b3_v;
  }

  public double getB4_p() {
    return b4_p;
  }

  public void setB4_p(double b4_p) {
    this.b4_p = b4_p;
  }

  public int getB4_v() {
    return b4_v;
  }

  public void setB4_v(int b4_v) {
    this.b4_v = b4_v;
  }

  public double getB5_p() {
    return b5_p;
  }

  public void setB5_p(double b5_p) {
    this.b5_p = b5_p;
  }

  public int getB5_v() {
    return b5_v;
  }

  public void setB5_v(int b5_v) {
    this.b5_v = b5_v;
  }

  public double getBid() {
    return bid;
  }

  public void setBid(double bid) {
    this.bid = bid;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public double getHigh() {
    return high;
  }

  public void setHigh(double high) {
    this.high = high;
  }

  public double getLow() {
    return low;
  }

  public void setLow(double low) {
    this.low = low;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public double getOpen() {
    return open;
  }

  public void setOpen(double open) {
    this.open = open;
  }

  public double getPre_close() {
    return pre_close;
  }

  public void setPre_close(double pre_close) {
    this.pre_close = pre_close;
  }

  public double getPrice() {
    return price;
  }

  public void setPrice(double price) {
    this.price = price;
  }

  public String getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = time;
  }

  public long getVolume() {
    return volume;
  }

  public void setVolume(long volume) {
    this.volume = volume;
  }
}
