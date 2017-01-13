package fsanalysis;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import net.sf.json.JSONArray;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datacrawler.HtmlAnalysis;
import db.DbConnection;

/**
 * Created by cy111966 on 2016/11/24.
 */
public class FsAnalysis {

  static Logger log = LoggerFactory.getLogger("logfile");
  static Logger log_error = LoggerFactory.getLogger("errorfile");
  public static final String title = "title,股票代码,开始时间,结束时间,买量,卖量,买卖差,买入金额,卖出金额,买卖金额差,增量资金";

  public static String outfile = "D:/stock_data/holders/fs_analysis_";

  public static Predicate<FsModel> b_filter = new Predicate<FsModel>() {
    public boolean apply(FsModel input) {
      return input.getType().equals("买盘");
    }
  };
  public static Predicate<FsModel> s_filter = new Predicate<FsModel>() {
    public boolean apply(FsModel input) {
      return input.getType().equals("卖盘");
    }
  };

  public static Predicate<FsModel> zx_filter = new Predicate<FsModel>() {
    public boolean apply(FsModel input) {
      return input.getType().equals("中性盘");
    }
  };


  public static void main(String[] args) {
    try {
      //List<FsModel> fsModels = query_data("002125","2016-11-21", "14:00:00", "2016-11-22", "10:25:00");
//      FsResDisplay fsResDisplay =
//          fs_analysis("002125", "2016-11-21", "14:00:00", "2016-11-22", "10:25:00", 10, Unit.min, 0);
      List<FsResDisplay> fsResDisplaysResList = new ArrayList<>();

      FsResDisplay fsResDisplay =
          fs_analysis_v("000798", "2016-11-16", "09:00:00", "2016-12-21", "15:00:00", 1, Unit.day,
                      120000,120000);

      FileWriter fw = null;
      BufferedWriter bfw = null;
      int count = 0;

      String final_file_name = outfile + fsResDisplay.getFile_name() + ".txt";
      try {
        fw = new FileWriter(final_file_name, true);
        bfw = new BufferedWriter(fw);
        printRes(title, bfw);
        FsRes lj_item = fsResDisplay.getLj_item();
        String lj_item_str = "total," + lj_item.toString();
        printRes(lj_item_str, bfw);
        List<FsRes> resList = fsResDisplay.getResList();
        for (FsRes fsRes : resList) {
          String zq_item_str = "zqitem," + fsRes.toString();
          printRes(zq_item_str, bfw);
          count++;
        }
        System.out.println("总计数:" + count);
        bfw.flush();//输出
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

      //excel输出
      try {
        String outfileExcel = outfile + fsResDisplay.getFile_name() + ".xls";
        FileOutputStream os = new FileOutputStream(outfileExcel);
        excel_output(fsResDisplay.getResList(), fsResDisplay.getLj_item(), os);
        os.close();
      } catch (Exception e) {
        log_error.error("excel 输出错误:", e);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * 查询分时数据
   */
  public static List<FsModel> query_data(String stockcode, String beginDate, String beginTime,
                                         String endDate, String endTime)
      throws SQLException {
    String begin = beginDate + " " + beginTime;
    String end = endDate + " " + endTime;
    final long begin_l = DateUtil.convert2long(begin, DateUtil.TIME_FORMAT);
    final long end_l = DateUtil.convert2long(end, DateUtil.TIME_FORMAT);
    List<FsModel> lists = new ArrayList<FsModel>();
    Connection conn = DbConnection.getConn();
    PreparedStatement ps =
        conn.prepareStatement(
            "SELECT * FROM db_test.fs_st_data where date >=? and date <=? and code = ?");
    ps.setString(1, beginDate);
    ps.setString(2, endDate);
    ps.setString(3, stockcode);
    ResultSet rs = ps.executeQuery();
    //int col = rs.getMetaData().getColumnCount();
    while (rs.next()) {
      String code = rs.getString(2);
      String date = rs.getString(3);
      String time = rs.getString(4);
      double price = rs.getDouble(5);
      String change = rs.getString(6);
      long volume = rs.getLong(7);
      long amount = rs.getLong(8);
      String type = rs.getString(9);
      String dateTime = date + " " + time;
      long time_long = DateUtil.convert2long(dateTime, DateUtil.TIME_FORMAT);
      FsModel model = new FsModel();
      model.setCode(code);
      model.setDate(date);
      model.setTime(time);
      model.setPrice(price);
      model.setChange(change);
      model.setVolume(volume);
      model.setAmount(amount);
      model.setType(type);
      model.setDateTime(time_long);
      lists.add(model);
//      for (int i=2;i<=col;i++){
//        System.out.print(rs.getString(i)+"\t");
//      }
//      System.out.println("");
    }
    //过滤
    Collection<FsModel> filterList = Collections2.filter(lists, new Predicate<FsModel>() {
      public boolean apply(FsModel input) {
        long dateTime = input.getDateTime();
        return dateTime >= begin_l && dateTime <= end_l;
      }
    });
    return Lists.newArrayList(filterList);
  }


  /**
   * 分析分时函数
   *
   * @param unit             (求和&&求平均)
   * @param volume(每个周期量的数组) 输出: 1.周期的买入/卖出/差额 2.累计买入/卖出/差额
   */
  public static FsResDisplay fs_analysis(String stockcode, String beginDate, String beginTime,
                                         String endDate, String endTime, int period, Unit unit,
                                         long volume)
      throws SQLException {
    String file_name = Joiner.on("_").join(stockcode, beginDate, endDate, period, unit, volume);
    final long filter_amount = volume;
    String begin = beginDate + " " + beginTime;
    String end = endDate + " " + endTime;
    long begin_l = DateUtil.convert2long(begin, DateUtil.TIME_FORMAT);
    long end_l = DateUtil.convert2long(end, DateUtil.TIME_FORMAT);
    //存储周期分析结果
    List<FsRes> fs_res_list = new ArrayList<FsRes>();
    //原始数据
    List<FsModel> queryList = query_data(stockcode, beginDate, beginTime, endDate, endTime);
    //处理量过滤
    Collection<FsModel> queryList_filter = Collections2.filter(queryList, new Predicate<FsModel>() {
      public boolean apply(FsModel input) {
        return input.getAmount() > filter_amount;
      }
    });
    //切分
    while (end_l >= begin_l) {
      Date begin_d = DateUtil.convert2date(begin_l);
      Date date_step = DateUtil.date_computer(begin_d, period, unit);
      long step_end = date_step.getTime();
      List<FsModel> qj_list = filter(Lists.newArrayList(queryList_filter), begin_l, step_end);
      //处理区间数据
      Collection<FsModel> qj_list_c =
          Collections2.transform(qj_list, new Function<FsModel, FsModel>() {
            public FsModel apply(FsModel input) {
              String change = input.getChange();
              if (change.equals("--")) {
                input.setChange("0");
              }
              return input;
            }
          });
      Collection<FsModel> b_col = Collections2.filter(qj_list_c, b_filter);
      Collection<FsModel> s_col = Collections2.filter(qj_list_c, s_filter);
      FsResTemp var = fs_computer(qj_list_c);
      long amount_var_sum = var.getAmount_var_sum();
      FsResTemp b = fs_computer(b_col);
      long amount_sum_b = b.getAmount_sum();
      long volume_sum_b = b.getVolume_sum();
      FsResTemp s = fs_computer(s_col);
      long amount_sum_s = s.getAmount_sum();
      long volume_sum_s = s.getVolume_sum();
      long volume_diff = volume_sum_b - volume_sum_s;
      long amount_diff = amount_sum_b - amount_sum_s;
      FsRes fsRes = new FsRes();
      fsRes.setBuy(volume_sum_b);
      fsRes.setSale(volume_sum_s);
      fsRes.setDiff_v(volume_diff);
      fsRes.setAmount_b(amount_sum_b);
      fsRes.setAmount_s(amount_sum_s);
      fsRes.setAmount_diff(amount_diff);
      fsRes.setAmount_var(amount_var_sum);
      fsRes.setBegin(DateUtil.convert2dateStr(begin_l));
      fsRes.setEnd(DateUtil.convert2dateStr(step_end));
      fsRes.setBegin_l(begin_l);
      fsRes.setEnd_l(end_l);
      //添加code
      fsRes.setCode(stockcode);
      fs_res_list.add(fsRes);
      begin_l = date_step.getTime();
    }
    System.err.println(fs_res_list.size());
    //过滤非交易时间段
    Collection<FsRes> fs_res_list_filter = Collections2.filter(fs_res_list, new Predicate<FsRes>() {
      public boolean apply(FsRes input) {
        // return input.getSale() != 0;//// TODO: 2016/11/25 处理是否合适
        return input.getAmount_diff() != 0;
        //return true;
      }
    });
    System.out.println(fs_res_list_filter.size());
    //计算累计量
    FsRes fsRes = fs_computer_sum(Lists.newArrayList(fs_res_list_filter));

    FsResDisplay resDisplay = new FsResDisplay();
    resDisplay.setLj_item(fsRes);
    resDisplay.setResList(Lists.newArrayList(fs_res_list_filter));
    resDisplay.setFile_name(file_name);
    return resDisplay;
  }

  /**
   * * 分析分时函数
   *
   * @param unit    (求和&&求平均)
   * @param svolume 输出: 1.周期的买入/卖出/差额 2.累计买入/卖出/差额
   */
  public static FsResDisplay fs_analysis_v(String stockcode, String beginDate, String beginTime,
                                           String endDate, String endTime, int period, Unit unit,
                                           long bVolume, long svolume)
      throws SQLException {
    String
        file_name =
        Joiner.on("_").join(stockcode, beginDate, endDate, period, unit, bVolume, svolume);
    final long filter_amount_b = bVolume;
    final long filter_amount_s = svolume;
    String begin = beginDate + " " + beginTime;
    String end = endDate + " " + endTime;
    long begin_l = DateUtil.convert2long(begin, DateUtil.TIME_FORMAT);
    long end_l = DateUtil.convert2long(end, DateUtil.TIME_FORMAT);
    //存储周期分析结果
    List<FsRes> fs_res_list = new ArrayList<FsRes>();
    //原始数据
    List<FsModel> queryList = query_data(stockcode, beginDate, beginTime, endDate, endTime);
    //处理量过滤&类型
    Collection<FsModel>
        queryList_filter_b =
        Collections2.filter(queryList, new Predicate<FsModel>() {
          public boolean apply(FsModel input) {
            return input.getAmount() > filter_amount_b && input.getType().equals("买盘");
          }
        });

    Collection<FsModel>
        queryList_filter_s =
        Collections2.filter(queryList, new Predicate<FsModel>() {
          public boolean apply(FsModel input) {
            return input.getAmount() > filter_amount_s && input.getType().equals("卖盘");
          }
        });
    List<FsModel> queryList_filter_all = new ArrayList<>();
    queryList_filter_all.addAll(queryList_filter_b);
    queryList_filter_all.addAll(queryList_filter_s);
    //切分
    while (end_l >= begin_l) {
      Date begin_d = DateUtil.convert2date(begin_l);
      Date date_step = DateUtil.date_computer(begin_d, period, unit);
      long step_end = date_step.getTime();
      List<FsModel> qj_list = filter(Lists.newArrayList(queryList_filter_all), begin_l, step_end);

      //处理区间数据
      Function<FsModel, FsModel> fuc_c = new Function<FsModel, FsModel>() {
        public FsModel apply(FsModel input) {
          String change = input.getChange();
          if (change.equals("--")) {
            input.setChange("0");
          }
          return input;
        }
      };
      Collection<FsModel> qj_list_c =
          Collections2.transform(qj_list, fuc_c);
      Collection<FsModel> b_col = Collections2.filter(qj_list_c, b_filter);
      Collection<FsModel> s_col = Collections2.filter(qj_list_c, s_filter);
      FsResTemp var = fs_computer(qj_list_c);
      long amount_var_sum = var.getAmount_var_sum();
      FsResTemp b = fs_computer(b_col);
      long amount_sum_b = b.getAmount_sum();
      long volume_sum_b = b.getVolume_sum();
      FsResTemp s = fs_computer(s_col);
      long amount_sum_s = s.getAmount_sum();
      long volume_sum_s = s.getVolume_sum();
      long volume_diff = volume_sum_b - volume_sum_s;
      long amount_diff = amount_sum_b - amount_sum_s;
      FsRes fsRes = new FsRes();
      fsRes.setBuy(volume_sum_b);
      fsRes.setSale(volume_sum_s);
      fsRes.setDiff_v(volume_diff);
      fsRes.setAmount_b(amount_sum_b);
      fsRes.setAmount_s(amount_sum_s);
      fsRes.setAmount_diff(amount_diff);
      fsRes.setAmount_var(amount_var_sum);
      fsRes.setBegin(DateUtil.convert2dateStr(begin_l));
      fsRes.setEnd(DateUtil.convert2dateStr(step_end));
      fsRes.setBegin_l(begin_l);
      fsRes.setEnd_l(end_l);
      //添加code
      fsRes.setCode(stockcode);
      fs_res_list.add(fsRes);
      begin_l = date_step.getTime();
    }
    System.err.println(fs_res_list.size());
    //过滤非交易时间段
    Collection<FsRes> fs_res_list_filter = Collections2.filter(fs_res_list, new Predicate<FsRes>() {
      public boolean apply(FsRes input) {
        // return input.getSale() != 0;//// TODO: 2016/11/25 处理是否合适
        return input.getAmount_diff() != 0;
        //return true;
      }
    });
    System.out.println(fs_res_list_filter.size());
    //计算累计量
    FsRes fsRes = fs_computer_sum(Lists.newArrayList(fs_res_list_filter));

    FsResDisplay resDisplay = new FsResDisplay();
    resDisplay.setLj_item(fsRes);
    resDisplay.setResList(Lists.newArrayList(fs_res_list_filter));
    resDisplay.setFile_name(file_name);
    return resDisplay;
  }

  public static List<FsModel> filter(List<FsModel> list, long begin, long end) {
    List<FsModel> resList = new ArrayList<FsModel>();
    for (FsModel item : list) {
      long dateTime = item.getDateTime();
      if (dateTime >= begin && dateTime <= end) {
        resList.add(item);
      }
    }
    return resList;
  }

  public static FsResTemp fs_computer(Collection<FsModel> cols) {
    FsResTemp resTemp = new FsResTemp();
    long sum1 = 0;
    long sum2 = 0;
    long sum3 = 0;
    for (FsModel item : cols) {
      String change = item.getChange();
      Double change_d = Double.valueOf(change);
      long volume = item.getVolume();
      long amount_var = (long) (change_d * volume * 100);
      long amount = item.getAmount();
      sum1 += volume;
      sum2 += amount;
      sum3 += amount_var;
    }
    resTemp.setVolume_sum(sum1);
    resTemp.setAmount_sum(sum2);
    resTemp.setAmount_var_sum(sum3);
    return resTemp;
  }

  public static FsRes fs_computer_sum(List<FsRes> items) {
    FsRes fsRes = new FsRes();
    long pre_b = 0;
    long pre_e = 0;
    long mix = 0;
    long max = 0;
    long buy_sum = 0;
    long sale_sum = 0;
    long diff_v_sum = 0;
    long amount_b_sum = 0;
    long amount_s_sum = 0;
    long amount_diff_sum = 0;
    long amount_var_sum = 0;
    String code = "";
    int i = 0;
    for (FsRes item : items) {
      long begin_l = item.getBegin_l();
      if (i == 0) {
        mix = begin_l;
        code = item.getCode();
      }
      if (pre_b > begin_l) {
        mix = begin_l;
      }
      long end_l = item.getEnd_l();
      if (pre_e < end_l) {
        max = end_l;
      }
      buy_sum += item.getBuy();
      sale_sum += item.getSale();
      diff_v_sum += item.getDiff_v();
      amount_b_sum += item.getAmount_b();
      amount_s_sum += item.getAmount_s();
      amount_diff_sum += item.getAmount_diff();
      amount_var_sum += item.getAmount_var();
      pre_b = begin_l;
      pre_e = end_l;
      i++;
    }
    fsRes.setCode(code);
    fsRes.setBegin(DateUtil.convert2dateStr(mix));
    fsRes.setEnd(DateUtil.convert2dateStr(max));
    fsRes.setBuy(buy_sum);
    fsRes.setSale(sale_sum);
    fsRes.setDiff_v(diff_v_sum);
    fsRes.setAmount_b(amount_b_sum);
    fsRes.setAmount_s(amount_s_sum);
    fsRes.setAmount_diff(amount_diff_sum);
    fsRes.setAmount_var(amount_var_sum);
    return fsRes;
  }

  public static void printRes(String resPrint, BufferedWriter write) {
    try {
      write.write(resPrint);
      write.newLine();
    } catch (IOException e) {
      log_error.error("append error context:{}", resPrint);
    }

  }

  public static void excel_output(List<FsRes> resList, FsRes total, OutputStream out)
      throws IOException {
    HSSFWorkbook wb = new HSSFWorkbook();
    HSSFSheet sheet = wb.createSheet();
    HSSFRow titleRow = sheet.createRow(0);//写标题
    titleRow.createCell(0, HSSFCell.CELL_TYPE_STRING).setCellValue("时间区间");
    titleRow.createCell(1, HSSFCell.CELL_TYPE_STRING).setCellValue("股票代码");
    titleRow.createCell(2, HSSFCell.CELL_TYPE_STRING).setCellValue("开始时间");
    titleRow.createCell(3, HSSFCell.CELL_TYPE_STRING).setCellValue("结束时间");
    titleRow.createCell(4, HSSFCell.CELL_TYPE_STRING).setCellValue("买量");
    titleRow.createCell(5, HSSFCell.CELL_TYPE_STRING).setCellValue("卖量");
    titleRow.createCell(6, HSSFCell.CELL_TYPE_STRING).setCellValue("买卖差");
    titleRow.createCell(7, HSSFCell.CELL_TYPE_STRING).setCellValue("买入金额");
    titleRow.createCell(8, HSSFCell.CELL_TYPE_STRING).setCellValue("卖出金额");
    titleRow.createCell(9, HSSFCell.CELL_TYPE_STRING).setCellValue("买卖金额差");
    titleRow.createCell(10, HSSFCell.CELL_TYPE_STRING).setCellValue("增量资金");

    HSSFRow totalRow = sheet.createRow(1);
    totalRow.createCell(0, HSSFCell.CELL_TYPE_STRING).setCellValue("T_All");
    totalRow.createCell(1, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getCode());
    totalRow.createCell(2, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getBegin());
    totalRow.createCell(3, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getEnd());
    totalRow.createCell(4, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getBuy());
    totalRow.createCell(5, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getSale());
    totalRow.createCell(6, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getDiff_v());
    totalRow.createCell(7, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getAmount_b());
    totalRow.createCell(8, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getAmount_s());
    totalRow.createCell(9, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getAmount_diff());
    totalRow.createCell(10, HSSFCell.CELL_TYPE_STRING).setCellValue(total.getAmount_var());
    HSSFRow row;
    int writeIndex = 2;
    for (FsRes fsRes : resList) {
      row = sheet.createRow(writeIndex);
      row.createCell(0, HSSFCell.CELL_TYPE_STRING).setCellValue("T" + (writeIndex - 1));
      row.createCell(1, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getCode());
      row.createCell(2, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getBegin());
      row.createCell(3, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getEnd());
      row.createCell(4, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getBuy());
      row.createCell(5, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getSale());
      row.createCell(6, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getDiff_v());
      row.createCell(7, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getAmount_b());
      row.createCell(8, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getAmount_s());
      row.createCell(9, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getAmount_diff());
      row.createCell(10, HSSFCell.CELL_TYPE_STRING).setCellValue(fsRes.getAmount_var());
      writeIndex++;
    }
    wb.write(out);
  }
}
