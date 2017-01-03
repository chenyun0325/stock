package stormpython;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import fsanalysis.DateUtil;
import fsrealanalysis.FsData;
import fsrealanalysis.FsIndexRes;
import fsrealanalysis.SingleFsDataProcess;
import fsrealanalysis.SlidingWindowPriceRes;

/**
 * Created by chenyun on 16/2/2.
 * 定时发射数据
 * http://www.2cto.com/net/201605/512041.html
 */
public class Bolt2 extends BaseBasicBolt {

  static Logger log_error = LoggerFactory.getLogger("errorfile");

  Map<String, List<FsData>> code_map = new ConcurrentHashMap<String, List<FsData>>();//存储股票分时数据

  // todo 第一key：业务规则组合,第二key：股票代码

  Map<String,Map<String, List<FsIndexRes>>> code_map_multi = new ConcurrentHashMap<>();

  Map<String, List<FsIndexRes>> code_yd_map = new ConcurrentHashMap<>();//存储股票压单数据

  Map<String, List<FsIndexRes>> code_td_map = new ConcurrentHashMap<>();//存储股票托单数据

  Map<String,String> code_hash_map = new ConcurrentHashMap<>();//存储上一个股票的分时hash,防止同一个数据进入多次

  SingleFsDataProcess process = new SingleFsDataProcess();//分时数据处理函数

  private double filter_per;//压单或者托单比例

  private double filter_mount;//压单或者托单金额

  private int slide_size;//窗口大小

  Map<String, SynchronizedDescriptiveStatistics> stats_map = new ConcurrentHashMap<>();

  Map<String, SynchronizedDescriptiveStatistics> stats_map_index = new ConcurrentHashMap<>();

  StandardDeviation std = new StandardDeviation();

  private String yd_file ="D:/stock_data/holders/yd";
  private String td_file ="D:/stock_data/holders/td";

  public Bolt2(double filter_mount, double filter_per, int slide_size) {
    this.filter_mount = filter_mount;
    this.filter_per = filter_per;
    this.slide_size = slide_size;
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      String code = input.getString(0);
      Object item = input.getValue(1);
      System.out.println(code);
      JSONObject item_json = JSONObject.fromObject(item);
      FsData fsdata = (FsData) JSONObject.toBean(item_json, FsData.class);
      code_map = transfer(code_map, code, fsdata);
      //处理单个数据----每新增1个数据分析一次
      FsIndexRes indexRes = this.process.process(fsdata);
      boolean b_ge_s = indexRes.isB_ge_s();
      double per = indexRes.getPer();
      double a_all_m = indexRes.getA_all_m();//可以利用平均值
      double b_all_m = indexRes.getB_all_m();
      // TODO: 2016/12/4 考虑涨停板和跌停板
      double price_dif = indexRes.getPrice_dif();//涨跌幅度
      if (b_ge_s) {
        if (per > filter_per && b_all_m > filter_mount) {//托单
          code_td_map = transferRes(code_td_map, code, indexRes);
          String dateStr= DateUtil.convert2dateStr(new Date());
          String file=td_file+"_"+filter_mount+"_"+dateStr+".txt";
          printRes(file,code_td_map);
        }
      } else {
        if (per > filter_per && a_all_m > filter_mount) {//压单
          code_yd_map = transferRes(code_yd_map, code, indexRes);
          String dateStr= DateUtil.convert2dateStr(new Date());
          String file=yd_file+"_"+filter_mount+"_"+dateStr+".txt";
          printRes(file,code_yd_map);
        }
      }

      // TODO: 2016/12/4 分析结果存储

      //处理窗口数据,价格
      double price = indexRes.getPer();
      double stamp = indexRes.getTime_stamp_long();
      SynchronizedDescriptiveStatistics stats = stats_map.get(code);
      SynchronizedDescriptiveStatistics stats_index = stats_map_index.get(code);
      SlidingWindowPriceRes price_var_res = null;
      if (stats == null) {
        stats = new SynchronizedDescriptiveStatistics(slide_size);
        stats_index = new SynchronizedDescriptiveStatistics(slide_size);
        stats_index.addValue(stamp);
        stats_map_index.put(code, stats_index);
        stats.addValue(price);
        stats_map.put(code, stats);
        String hash = indexRes.getTime_stamp_long() +"_"+ code;
        code_hash_map.put(code,hash);
      } else {
        String pre_hash = code_hash_map.get(code);
        String hash = indexRes.getTime_stamp_long() +"_"+ code;
        if (!hash.equals(pre_hash)){
        stats.addValue(price);
        stats_index.addValue(stamp);
          code_hash_map.put(code,hash);
        }
      }
      long n = stats.getN();//管道中的数量
      if (n == slide_size) {//积累达到一定量才计算方差
        double[] var_trans = new double[slide_size];
        double sum = stats.getSum();
        double[] values = stats.getValues();
        for (int i = 0; i < slide_size; i++) {
          var_trans[i] = values[i] / sum;
        }
        double var_p = std.evaluate(var_trans);
        price_var_res = new SlidingWindowPriceRes();
        long start_stamp = (long) stats_index.getMin();
        price_var_res.setStart(start_stamp);
        price_var_res.setEnd((long) stamp);
        price_var_res.setVar_p(var_p);
        price_var_res.setDatas(values);

      }

      // TODO: 2016/12/3 下一个bolt进行时序数据分析
      //夹单条件:a1_p>0.9&b1_p>0.9&jd_per=1?? FileWriter fw = null;

      MessageId messageId = input.getMessageId();
      System.err.println(code);
      //System.err.println("对消息加工第1次-------[arg0]:" + code + "---[arg2]:" + item + "------->" + code);
      if (code != null) {
        collector.emit(new Values(code, indexRes, price_var_res));
      }
    } catch (Exception e) {
      log_error.error("unexcept error:",e);
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("code", "index", "price_var"));
  }

  public Map<String, List<FsData>> transfer(Map<String, List<FsData>> map, String code,
                                            FsData item) {
    List<FsData> fsDatas = map.get(code);
    String pre_hash = code_hash_map.get(code);
    String hash = DateUtil.convert2long(item.getDate()+" "+item.getTime(),DateUtil.TIME_FORMAT)+ "_" + code;
    if (fsDatas != null) {
      if (!hash.equals(pre_hash)) {
        fsDatas.add(item);
      }
    } else {
      List<FsData> fs_list = new ArrayList<FsData>();
      fs_list.add(item);
      map.put(code, fs_list);
      code_hash_map.put(code,hash);
    }
    return map;
  }

  public Map<String, List<FsIndexRes>> transferRes(Map<String, List<FsIndexRes>> map, String code,
                                                   FsIndexRes item) {

    List<FsIndexRes> fsDatas = map.get(code);
    String pre_hash = code_hash_map.get(code);
    String hash = item.getTime_stamp_long() +"_"+ code;
    if (fsDatas != null) {
      if (!hash.equals(pre_hash)) {
        fsDatas.add(item);
      }
    } else {
      List<FsIndexRes> fs_list = new ArrayList<FsIndexRes>();
      fs_list.add(item);
      map.put(code, fs_list);
      code_hash_map.put(code,hash);
    }
    return map;
  }

  //    public static void printRes(String resPrint, BufferedWriter write) {
//        try {
//            write.write(resPrint);
//            write.newLine();
//        } catch (IOException e) {
//            log_error.error("append error context:{}", resPrint);
//        }
//
//    }
  public void printRes(String fileName, Map<String, List<FsIndexRes>> maps) {
    FileWriter fw = null;
    BufferedWriter bfw = null;
    try {
      fw = new FileWriter(fileName, false);
      bfw = new BufferedWriter(fw);
      long time = System.currentTimeMillis();
      String timeStr = DateUtil.convert2dateStr(time);
      for (String code : maps.keySet()) {
        List<FsIndexRes> list = maps.get(code);
        String json = JSONArray.fromObject(list).toString();
        String item = code + ":" + "@" + "time:" + timeStr + "@" + "content:" + json;
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
