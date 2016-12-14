package stormpython;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import fsrealanalysis.FsIndexRes;
import fsrealanalysis.SlidingWindowPriceRes;

/**
 * Created by cy111966 on 2016/12/3.
 * 1.计算方差
 * 2.滑动窗口算法
 * 3.math common
 * http://www.bubuko.com/infodetail-542704.html
 * http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math4/stat/descriptive/DescriptiveStatistics.html
 */
public class SlidingWindowBolt extends BaseBasicBolt {
  static Logger log_error = LoggerFactory.getLogger("errorfile");

  Map<String ,Deque<FsIndexRes>> code_wid_index_map = new ConcurrentHashMap<>();

  Map<String ,Deque<SlidingWindowPriceRes>> code_wid_price_map = new ConcurrentHashMap<>();

  static Map<String ,List<SlidingWindowPriceRes>> code_jd_map = new ConcurrentHashMap<>();//存储股票夹单数据

  static Map<String ,List<SlidingWindowPriceRes>> code_jd_his_map = new ConcurrentHashMap<>();//存储股票夹单历史数据

  private int maxLen;//需要清理list的最大长度

  private String jd_file ="D:/stock_data/holders/jd.txt";
  private static  String jd_file_thead ="D:/stock_data/holders/jd_thead.txt";
  private static  String jd_his_file_thead ="D:/stock_data/holders/jd_his_thead.txt";


  private int max_size;//最大大小
  private int  wind_size;//窗口大小
  private double price_dif_var ;//方差大小
  private double price_diff_var1;
  private double amount;//金额大小
  private double amount1;//金额大小1 // TODO: 2016/12/6 不同业务参数区间不同输出


  static {
     new Thread("data_out") {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(5000);
            printResThead(jd_file_thead, code_jd_map,false);
            //设置执行时间
            Calendar calendar = Calendar.getInstance();
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH);
            int day = calendar.get(Calendar.DAY_OF_MONTH);//每天
            //定制每天的15:02:00执行，
            calendar.set(year, month, day, 15, 2, 00);
            Date date = calendar.getTime();
            //定点执行任务
            TimerTask timerTask=  new TimerTask() {
              @Override
              public void run() {
                printResThead(jd_his_file_thead,code_jd_his_map,true);
                code_jd_his_map.clear();//保存之后清空数据
              }
            };
            Timer timer = new Timer();
            timer.schedule(timerTask,date);
          } catch (Exception e) {
            log_error.error("data_out error:", e);
            e.printStackTrace();
            continue;
          }
        }
      }
    }.start();
  }
  public SlidingWindowBolt(int max_size, int wind_size,double price_dif_var,double amount ) {
    this.max_size = max_size;
    this.wind_size = wind_size;
    this.price_dif_var=price_dif_var;
    this.amount=amount;
  }



  public void execute(Tuple input, BasicOutputCollector collector) {

    try {
      String code = input.getString(0);
      Object index = input.getValue(1);
      Object price_var = input.getValue(2);
      //数据存储以用于后续窗口分析
      JSONObject index_json = JSONObject.fromObject(index);
      FsIndexRes fsIndexRes = (FsIndexRes) JSONObject.toBean(index_json, FsIndexRes.class);

      Deque<FsIndexRes> fsDatas = code_wid_index_map.get(code);
      if (fsDatas != null) {
        int size = fsDatas.size();
        fsDatas.offerLast(fsIndexRes);
        if (size>=max_size){
          fsDatas.pollFirst();
        }
      }else {
        Deque<FsIndexRes> fs_list = new LinkedList<>();
        fs_list.offerLast(fsIndexRes);
        code_wid_index_map.put(code,fs_list);
      }
      if (price_var != null) {
        JSONObject price_var_json = JSONObject.fromObject(price_var);
        SlidingWindowPriceRes priceRes = (SlidingWindowPriceRes) JSONObject.toBean(price_var_json, SlidingWindowPriceRes.class);

        Deque<SlidingWindowPriceRes> priceResQ = code_wid_price_map.get(code);
        if (priceResQ != null) {
          int size = priceResQ.size();
          priceResQ.offerLast(priceRes);
          if (size>=max_size){
            priceResQ.pollFirst();
          }
        }else {
          Deque<SlidingWindowPriceRes> price_list = new LinkedList<>();
          price_list.offerLast(priceRes);
          code_wid_price_map.put(code,price_list);
        }
        //夹单条件:a1_p>0.9&b1_p>0.9&jd_per=1
        //分析夹单模式
        double a_var = fsIndexRes.getA_var();
        double b_var = fsIndexRes.getB_var();
        double var_p = priceRes.getVar_p();//价格波动小
        double a_all_m = fsIndexRes.getA_all_m();//金额
        double b_all_m = fsIndexRes.getB_all_m();
        if (a_all_m > amount && b_all_m > amount && var_p < price_dif_var) {
          code_jd_map = transferRes(code_jd_map,code,priceRes);
          printRes(jd_file,code_jd_map);
        }

      }


      //窗口分析------长时间段趋势
    }catch (Exception e){
      e.printStackTrace();
      //停牌股票
    }

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  public Map<String,List<SlidingWindowPriceRes>> transferRes(Map<String,List<SlidingWindowPriceRes>> map,String code,SlidingWindowPriceRes item){

    List<SlidingWindowPriceRes> fsDatas = map.get(code);
    int size = fsDatas.size();
    if (size > maxLen) {
      code_jd_his_map.put(code,fsDatas);//存储历史数据
      fsDatas = new ArrayList<>();//赋予新对象
    }
    if (fsDatas != null) {
      fsDatas.add(item);
      if (size > maxLen) {
        map.put(code,fsDatas);
      }
    }else {
      List<SlidingWindowPriceRes> fs_list = new ArrayList<>();
      fs_list.add(item);
      map.put(code,fs_list);
    }
    return map;
  }
  public void printRes(String fileName, Map<String, List<SlidingWindowPriceRes>> maps) {
    FileWriter fw = null;
    BufferedWriter bfw = null;
    try {
      fw = new FileWriter(fileName, false);
      bfw = new BufferedWriter(fw);
      long time = System.currentTimeMillis();
      for (String code : maps.keySet()) {
        List<SlidingWindowPriceRes> list = maps.get(code);
        String json = JSONArray.fromObject(list).toString();
        String item = code + ":" + "@" + "time:" + time + "@" + "content:" + json;
        bfw.write(item);
        bfw.newLine();
      }
      bfw.flush();

    } catch (IOException e) {
      log_error.error("create file error", e);
    } finally {
      try {
        bfw.close();
        fw.close();
      } catch (IOException e) {
        log_error.error("close file error:", e);
      }
    }
  }
  public static void printResThead(String fileName, Map<String, List<SlidingWindowPriceRes>> maps,boolean appendFlag) {
    FileWriter fw = null;
    BufferedWriter bfw = null;
    try {
      fw = new FileWriter(fileName, appendFlag);
      bfw = new BufferedWriter(fw);
      long time = System.currentTimeMillis();
      for (String code : maps.keySet()) {
        List<SlidingWindowPriceRes> list = maps.get(code);
        String json = JSONArray.fromObject(list).toString();
        String item = code + ":" + "@" + "time:" + time + "@" + "content:" + json;
        bfw.write(item);
        bfw.newLine();
      }
      bfw.flush();

    } catch (IOException e) {
      log_error.error("create file error", e);
    } finally {
      try {
        bfw.close();
        fw.close();
      } catch (IOException e) {
        log_error.error("close file error:", e);
      }
    }
  }

}
