package datacrawler;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by cy111966 on 2016/11/15.
 */
public class HtmlAnalysis {

  // TODO: 2016/11/17 1.新进股东样本剔除限制股东 2.标记最新一期和其它期别重复股东的性质
  static Logger log = LoggerFactory.getLogger("logfile");
  static Logger log_error = LoggerFactory.getLogger("errorfile");
  public static final String dir = "D:/stock_data/holders";

  public static final String outfile = "D:/stock_data/holders/stocks_analysis.txt";

  public static final String outfileExcel = "D:/stock_data/holders/stocks_analysis.xls";


  public static final String split="@";

  public static String format1 =
      "{org:%s,num:%s,change:%s,per:%s,esCost:%s,perChange:%s,type:%s,des:%s}";

  public static String format2 = "{org:%s,num:%s,change:%s,per:%s,perChange:%s,type:%s,des:%s}";

  public static String format_print = "{股票代码期数1:%s,股票代码期数2:%s,共同股东:%s}";

  public static Predicate<SdLtHolder> xjFilter = new Predicate<SdLtHolder>() {
    public boolean apply(SdLtHolder input) {
      return input.getChange().equals("新进");
    }
  };

  public static Predicate<SdLtHolder> noFilter = new Predicate<SdLtHolder>() {
    public boolean apply(SdLtHolder input) {
      return true;
    }
  };

  public static Predicate<SdLtHolder> excludeTcFilter = new Predicate<SdLtHolder>() {
    public boolean apply(SdLtHolder input) {
      return input.getChange().equals("退出");
    }
  };


  public static Predicate<SdLtHolderAnalysisRes>
      resFilter =
      new Predicate<SdLtHolderAnalysisRes>() {
        public boolean apply(SdLtHolderAnalysisRes input) {
          Set<String> cosSet = input.getCrossName();
          Set<String> filterSet = new HashSet<String>();
          for (String item : cosSet) {
            String[] split = item.split("___");
            if (split[0].length()<=4){//过滤条件
              filterSet.add(item);
            }
          }
          return filterSet.size() > 1;
        }
      };
  public static Predicate<SdLtHolderPerTimeContain>
      conFilter =
      new Predicate<SdLtHolderPerTimeContain>() {
        public boolean apply(SdLtHolderPerTimeContain input) {
          return input.getSingleNew() != null&&input.getSingleAll()!=null;
        }
      };

  public static void main(String[] args) {
    // String[] codes = {"600051","002125"};
    String[] codes = Constant.stock_all.split(",");
    String div_c = "bd_1";
    //最终产生两个set
    Map<String, Set<String>> newNameSet = Maps.newHashMap();
    Map<String, Set<String>> allNameSet = Maps.newHashMap();

    //存储最终分析结果
    List<SdLtHolderAnalysisRes> analysisResList = new ArrayList<SdLtHolderAnalysisRes>();
    //所有股票数据容器----(含最新和所有)
    List<SdLtHolderPerTimeContain> codesContains =
        new ArrayList<SdLtHolderPerTimeContain>();
    for (String code : codes) {
      String fileName = dir + "/" + code + ".html";
      //处理单支股票数据----(提取数据转换为对象)
      SdLtHolderPerTimeContain singleContain = analysisDoc(fileName, div_c, code);
      codesContains.add(singleContain);
    }


    //名称处理
    for (SdLtHolderPerTimeContain contain : Collections2.filter(codesContains, conFilter)) {//添加异常过滤
      SdLtHolderPerTime singleNew = contain.getSingleNew();
      List<SdLtHolderPerTime> singleAll = contain.getSingleAll();
      //处理最新数据
      newNameSet = genKeySet(singleNew, newNameSet, xjFilter);
      //处理所有数据
      for (SdLtHolderPerTime single : singleAll) {
        allNameSet = genKeySet(single, allNameSet, noFilter);
        //allNameSet = genKeySet(single, allNameSet, excludeTcFilter);
      }
    }
    //以newNameSet为基础,循环遍历allNameSet---->丢弃key相同记录
    Set<String> newNameKeySet = newNameSet.keySet();
    Set<String> allNameKeySet = allNameSet.keySet();
    Set<String> crossRes = new HashSet<String>();
    for (String newNameKey : newNameKeySet) {//外循环
      Set<String> baseSet = newNameSet.get(newNameKey);
      //分解set--->map
      Map<String, String> baseSetMap = setToMap(baseSet);
      Set<String> baseSetForComp = baseSetMap.keySet();
      for (String allNameKey : allNameKeySet) {//内循环
        crossRes.clear();
        crossRes.addAll(baseSetForComp);
        if (newNameKey.equals(allNameKey)) {//key相同不比较
          continue;
        }
        Set<String> compareSet = allNameSet.get(allNameKey);
        final Map<String, String> compareSetMap = setToMap(compareSet);
        Set<String> compareSetForComp = compareSetMap.keySet();
        //交集运算
        crossRes.retainAll(compareSetForComp);
        System.out.println("baseSet和compareSet交集" + crossRes);
        if (crossRes.size() > 0) {//交集非空
          SdLtHolderAnalysisRes analysisRes = new SdLtHolderAnalysisRes();
          Set<String> cross = Sets.newHashSet(crossRes);
          //从compareSetMap获取共同股东的性质
          Collection<String> crossFinal =
              Collections2.transform(cross, new Function<String, String>() {
                public String apply(String key) {
                  String type = compareSetMap.get(key);
                  return key + "___" + type;
                }
              });
          analysisRes.setCrossName(Sets.newHashSet(crossFinal));
          analysisRes.setKey1(newNameKey);
          analysisRes.setKey2(allNameKey);
          analysisResList.add(analysisRes);
        }
      }
    }
    //存储计算结果---analysisResList
    FileWriter fw = null;
    BufferedWriter bfw = null;
    int stock_count=0;
    try {
      fw = new FileWriter(outfile,false);
      bfw = new BufferedWriter(fw);
      List<String> reduceList = reduce(Collections2.filter(analysisResList, resFilter));
      Collections.sort(reduceList, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return o2.length()-o1.length();
        }
      });
      for (String printStr : reduceList) {
        bfw.write(printStr);
        bfw.newLine();
      }
//      for (SdLtHolderAnalysisRes retainItem : Collections2
//          .filter(analysisResList, resFilter)) {
//        printRes(retainItem,bfw);
//        stock_count++;
//      }
      bfw.flush();//输出
    } catch (IOException e) {
      log_error.error("create file error",e);
    }finally {
      try {
        bfw.close();
        fw.close();
      } catch (IOException e) {
        log_error.error("close file error:",e);
      }
    }
    //excel输出
    try {
      FileOutputStream os = new FileOutputStream(outfileExcel);
      excel_output(Collections2.filter(analysisResList, resFilter),os);
     os.close();
    } catch (Exception e) {
      log_error.error("excel 输出错误:",e);
    }

  }

  /**
   * 提取文件中指定区块
   */
  public static SdLtHolderPerTimeContain analysisDoc(String url, String id, String code) {
    File input = new File(url);
    SdLtHolderPerTimeContain contain = new SdLtHolderPerTimeContain();
    Element div = null;
    try {
      Document doc = Jsoup.parse(input, "utf-8");
      div = doc.getElementById(id);//bd_1
      if (div == null) {
        String[] params = Arrays.asList(url, id, code).toArray(new String[3]);
        log.error("没有流通股东信息------------url:{},id:{},code:{}", params);
        return contain;
      }
      Elements tabs = div.select("a.fdates");
      // List<String> div_c_set = new ArrayList<String>();
      int count = 0;
      SdLtHolderPerTime singleNew = null;
      List<SdLtHolderPerTime> singleAll = new ArrayList<SdLtHolderPerTime>();
      for (Element tab : tabs) {
        SdLtHolderPerTime codeSingleRecord = new SdLtHolderPerTime();
        List<SdLtHolder> allHolderList = new ArrayList<SdLtHolder>();
        String date = tab.text();
        System.out.println(date);
        String div_id = tab.attr("targ");
        //System.out.println(div_id);
        System.err.println(div_id);
        //analysisDoc_sub_head(div_sub, div, "table.ggintro", "thead");
        List<SdLtHolder> ltHolderList = analysisDoc_sub_body(div_id, div, "table.ggintro", "tr");
        System.err.println("-----out");
        List<SdLtHolder> tcHolderList =
            analysisDoc_sub_body(div_id, div, "table.m_table", "tr.gray");
        //合并
        allHolderList.addAll(ltHolderList);
        allHolderList.addAll(tcHolderList);

        //构建一期返回
        codeSingleRecord.setCode(code);
        codeSingleRecord.setDate(date);
        codeSingleRecord.setLists(allHolderList);
        //div_c_set.add(div_id);
        if (count == 0) {
          singleNew = codeSingleRecord;
        }
        singleAll.add(codeSingleRecord);
        count++;
      }

      contain.setSingleNew(singleNew);
      contain.setSingleAll(singleAll);
    } catch (Exception e) {
      e.printStackTrace();
      String[] params = Arrays.asList(url, id, code).toArray(new String[3]);
      log.error("url:{},id:{},code:{},div:{}", params);
      log_error.error("analysisDoc error:", e);
    }
    return contain;
  }

  public static void analysisDoc_sub_head(String div_sub, Element div, String first_sel,
                                          String second_sel) {
    Element div_ele = div.getElementById(div_sub);
    Elements heads = div_ele.select(first_sel).select(second_sel);//ggintro/m_table
    for (Element head : heads) {
      Elements trs = head.select("tr");
      for (Element tr : trs) {
        Elements tds = tr.select("th");
        for (Element td : tds) {
          String text = td.text();
          System.out.println(text);
        }
      }
      System.out.println("---------------------xxx");
    }
  }

  public static List<SdLtHolder> analysisDoc_sub_body(String div_sub_body, Element div,
                                                      String first_sel,
                                                      String tr_sel) {
    Element div_ele = div.getElementById(div_sub_body);
    Elements bodys = div_ele.select(first_sel).select("tbody");
    System.out.println(bodys.size());
    List<SdLtHolder> sdLtHolderslist = new ArrayList<SdLtHolder>();
    for (Element body : bodys) {
      Elements trs = body.select(tr_sel);
      for (Element tr : trs) {
        List<String> tdArray = new ArrayList<String>();
        //处理头
        Elements ths = tr.select("th");
        for (Element th : ths) {
          String text = th.text();
          System.out.println(text);
          tdArray.add(text);
        }
        //处理其它
        Elements tds = tr.select("td");
        for (Element td : tds) {
          String text = td.text();
          System.out.println(text);
          tdArray.add(text);
        }
        System.out.println("------------------body tr");
        Collection<String> tdCollections =
            Collections2.transform(tdArray, new Function<String, String>() {
              public String apply(String input) {
                return "\"" + input.replaceAll("\"","") + "\"";
              }
            });
        int size = tdCollections.size();
        String[] tdArrayTrs = new String[size];
        tdCollections.toArray(tdArrayTrs);
        String jsonStr = "";
        if (size == 7) {
          jsonStr = String.format(format2, tdArrayTrs);//转换为json格式
        } else {
          jsonStr = String.format(format1, tdArrayTrs);//转换为json格式
        }

        JSONObject recordJson = JSONObject.fromObject(jsonStr);
        SdLtHolder record = (SdLtHolder) JSONObject.toBean(recordJson, SdLtHolder.class);
        sdLtHolderslist.add(record);
      }

    }
    return sdLtHolderslist;
  }

  /**
   *
   * @param holderPerTime
   * @param resMap
   * @param filter
   * @return
   */
  public static Map<String, Set<String>> genKeySet(SdLtHolderPerTime holderPerTime,
                                                   Map<String, Set<String>> resMap,
                                                   Predicate filter) {
    String code = holderPerTime.getCode();
    String date = holderPerTime.getDate();
    String key = code + "_" + date;
    List<SdLtHolder> singleHolders = holderPerTime.getLists();
    Collection<SdLtHolder> filterCollection = Collections2.filter(singleHolders, filter);
    Collection<String> orgCollection =
        Collections2.transform(filterCollection, new Function<SdLtHolder, String>() {
          public String apply(SdLtHolder input) {
            return input.getOrg()+split+input.getChange();
          }
        });
    HashSet<String> orgSet = Sets.newHashSet(orgCollection);
    resMap.put(key, orgSet);
    return resMap;
  }

  public static void printRes(SdLtHolderAnalysisRes res, BufferedWriter write) {
    String key1 = res.getKey1();
    String key2 = res.getKey2();
    String holders = JSONArray.fromObject(res.getCrossName()).toString();
    String resPrint = String.format(format_print, key1, key2, holders);
    try {
      write.write(resPrint);
      write.newLine();
    } catch (IOException e) {
      log_error.error("append error context:{}",resPrint);
    }

    //System.err.println("-------------------begin");
    System.out.println(resPrint);
    //System.err.println("-------------------end");
  }

  public static Map<String,String> setToMap(Set<String> set){
    Map<String, String> maps = new HashMap<String, String>();
    for (String item : set) {
      String[] array = item.split(split);
      maps.put(array[0],array[1]);
    }
    return maps;
  }

  public static List<String> reduce(Collection<SdLtHolderAnalysisRes> resList){
    List<String> reduceList = new ArrayList<>();
    Map<String, List<SdLtHolderAnalysisRes>> hashMap = new HashMap<>();
    for (SdLtHolderAnalysisRes item : resList) {
      String key1 = item.getKey1();
      List<SdLtHolderAnalysisRes> keyList = hashMap.get(key1);
      if (keyList == null) {
        keyList = new ArrayList<>();
        keyList.add(item);
      }else {
        keyList.add(item);
      }
      hashMap.put(key1,keyList);
    }
    for (String key : hashMap.keySet()) {
      List<SdLtHolderAnalysisRes> keyList = hashMap.get(key);
      List<String> keys = new ArrayList<>();
      List<String> orgs = new ArrayList<>();
      for (SdLtHolderAnalysisRes item : keyList) {
        String key2 = item.getKey2();
        String org = JSONArray.fromObject(item.getCrossName()).toString();
        keys.add(key2);
        orgs.add(org);
      }
      String resPrint = String.format(format_print, key, Joiner.on("#").join(keys), Joiner.on("#").join(orgs));
      reduceList.add(resPrint);
    }
    return reduceList;
  }

  public static void excel_output(Collection<SdLtHolderAnalysisRes> resList, OutputStream out)
      throws IOException {
    Map<String, List<SdLtHolderAnalysisRes>> hashMap = new HashMap<>();
    for (SdLtHolderAnalysisRes item : resList) {
      String key1 = item.getKey1();
      List<SdLtHolderAnalysisRes> keyList = hashMap.get(key1);
      if (keyList == null) {
        keyList = new ArrayList<>();
        keyList.add(item);
      }else {
        keyList.add(item);
      }
      hashMap.put(key1,keyList);
    }
    HSSFWorkbook wb = new HSSFWorkbook();
    HSSFSheet sheet = wb.createSheet();
    HSSFRow titleRow = sheet.createRow(0);//写标题
    HSSFCell cell0 = titleRow.createCell(0, HSSFCell.CELL_TYPE_STRING);
    HSSFCell cell1 = titleRow.createCell(1, HSSFCell.CELL_TYPE_STRING);
    HSSFCell cell2 = titleRow.createCell(2, HSSFCell.CELL_TYPE_STRING);
    HSSFCell cell3 = titleRow.createCell(3, HSSFCell.CELL_TYPE_STRING);
    HSSFCell cell4 = titleRow.createCell(4, HSSFCell.CELL_TYPE_STRING);
    HSSFCell cell5 = titleRow.createCell(5, HSSFCell.CELL_TYPE_STRING);
    cell0.setCellValue("股票代码");
    cell1.setCellValue("被匹配股票代码");
    cell2.setCellValue("日期");
    cell3.setCellValue("共同股东");
    cell4.setCellValue("数量");
    cell5.setCellValue("排序字段");
    int writeIndex=1;//写入位置
    HSSFRow row ;
    for (String key : hashMap.keySet()) {
      //处理基期数据
      String[] code_date = key.split("_");
      row = sheet.createRow(writeIndex);
      row.createCell(0, HSSFCell.CELL_TYPE_STRING).setCellValue(code_date[0]);
      row.createCell(2, HSSFCell.CELL_TYPE_STRING).setCellValue(code_date[1]);
      writeIndex++;
      List<SdLtHolderAnalysisRes> keyList = hashMap.get(key);
      row.createCell(5,HSSFCell.CELL_TYPE_STRING).setCellValue(keyList.size());
      for (SdLtHolderAnalysisRes item : keyList) {
        String key2 = item.getKey2();
        String org = JSONArray.fromObject(item.getCrossName()).toString();
        String[] code_date_sp = key2.split("_");
        row = sheet.createRow(writeIndex);
        row.createCell(1, HSSFCell.CELL_TYPE_STRING).setCellValue(code_date_sp[0]);
        row.createCell(2, HSSFCell.CELL_TYPE_STRING).setCellValue(code_date_sp[1]);
        row.createCell(3,HSSFCell.CELL_TYPE_STRING).setCellValue(org);
        row.createCell(4,HSSFCell.CELL_TYPE_STRING).setCellValue(item.getCrossName().size());
        row.createCell(5,HSSFCell.CELL_TYPE_STRING).setCellValue(keyList.size());
        writeIndex++;
      }
    }
    wb.write(out);
  }

}
