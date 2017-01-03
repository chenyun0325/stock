package lhbanalysis;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.sf.json.JSONArray;

import org.apache.commons.lang.time.DateUtils;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import datacrawler.HtmlAnalysis;
import datacrawler.SdLtHolderAnalysisRes;
import fsanalysis.DateUtil;

/**
 * Created by cy111966 on 2016/12/19.
 */
public class LhbAnalysis {

  static Logger log = LoggerFactory.getLogger("logfile");
  static Logger log_error = LoggerFactory.getLogger("errorfile");
  public static String format_print = "{股票代码期数1:%s,股票代码期数2:%s,共同机构:%s}";
  public static final String dir = "D:/stock_data/lhb";
  public static String lhblistUrl = "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_js?t=2&c=&b=%s&e=%s&p=1&l=&ol=6&o=desc";
  public static String lhbJGUrl = "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_xx_js?c=%s&b=%s&l=%s";
  public static Predicate<LhbJGitem> nofilter = new Predicate<LhbJGitem>() {
    @Override
    public boolean apply(LhbJGitem input) {
      return true;
    }
  };
  public static Predicate<SdLtHolderAnalysisRes> resFilter = new Predicate<SdLtHolderAnalysisRes>() {
        public boolean apply(SdLtHolderAnalysisRes input) {
          Set<String> cosSet = input.getCrossName();
          Set<String> filterSet = new HashSet<String>();
          for (String item : cosSet) {
            String[] split = item.split("___");
            if (!split[0].contains("机构专用")){//过滤条件
              filterSet.add(item);
            }
          }
          return filterSet.size() > 1;
        }
      };
  public static void main(String[] args) {
    try {
      //1.加载数据
      String start="20161008";
      String end="20161229";
      Map<String, List<LhbItemJG>> loadMap = load(start, end);
      //2.构造比较集合
      TreeMap<String, List<LhbItemJG>> treeMap = new TreeMap<>();
      treeMap.putAll(loadMap);
      Set<String> keySet = treeMap.keySet();
      String firstKey = treeMap.firstKey();
      String lastKey = treeMap.lastKey();
      List<LhbItemJG> allList = new ArrayList<>();
      for (String key : keySet) {
        allList.addAll(treeMap.get(key));
      }
      List<LhbItemJG> newList = treeMap.get(lastKey);

      //最终产生两个set
      Map<String, Set<String>> newNameSet = Maps.newHashMap();
      Map<String, Set<String>> allNameSet = Maps.newHashMap();
      newNameSet = genKeySet(newList, newNameSet, nofilter);
      allNameSet = genKeySet(allList, allNameSet, nofilter);

      //以newNameSet为基础,循环遍历allNameSet---->丢弃key相同记录
      Set<String> newNameKeySet = newNameSet.keySet();
      Set<String> allNameKeySet = allNameSet.keySet();
      Set<String> crossRes = new HashSet<>();
      List<SdLtHolderAnalysisRes> analysisResList = new ArrayList<>();
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

        //3.结果存储
      FileWriter fw = null;
      BufferedWriter bfw = null;
      int stock_count=0;
      try {
        String outfile =dir+"/"+"res_"+firstKey+"_"+lastKey+".txt";
        fw = new FileWriter(outfile,false);
        bfw = new BufferedWriter(fw);
        Collection<SdLtHolderAnalysisRes> resFilterCol = Collections2.filter(analysisResList, resFilter);
        List<String> reduceList = reduce(resFilterCol);
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
//        for (SdLtHolderAnalysisRes retainItem : Collections2
//            .filter(analysisResList, resFilter)) {
//          printRes(retainItem,bfw);
//          stock_count++;
//        }
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
        String outfileExcel =dir+"/"+"res_"+firstKey+"_"+lastKey+".xls";
        FileOutputStream os = new FileOutputStream(outfileExcel);
        HtmlAnalysis.excel_output(Collections2.filter(analysisResList, resFilter),os);
        os.close();
      } catch (Exception e) {
        log_error.error("excel 输出错误:",e);
      }

      }catch(IOException e){
        log_error.error("", e);
      }

  }


  public static Map<String,List<LhbItemJG>> load(String begin, String end) throws IOException {
    //0.分解日期
    List<String> dateSet = new ArrayList<>();
    Date startDate = DateUtil.convert2date(begin);
    Date endDate = DateUtil.convert2date(end);
    for (Date start = startDate; start.before(endDate); start = DateUtils.addDays(start, 1)) {
      String startStr = DateUtil.convert2dateStr(start);
      dateSet.add(startStr);
    }
    //1.从硬盘指定目录加载文件或者网络下载数据
    Map<String,List<LhbItemJG>> mapCol = new HashMap<>();
    for (String date : dateSet) {
      String fileUri = dir+"/"+date;
      boolean flag = fileExist(fileUri);
      List<LhbItemJG> lhbList =null;
      if (flag) {
        lhbList = loadFile(fileUri);
      }else {
        try {//从网络加载数据,处理网络异常
          lhbList = new ArrayList<>();
          String listUrl = String.format(lhblistUrl, new String[]{date, date});
          String body = Jsoup.connect(listUrl).timeout(10000).ignoreContentType(true).execute().body();
          System.out.println(body);
          Pattern pattern = Pattern.compile("(?<=\\[)(.+?)(?=\\])");
          int b = body.indexOf("[");
          int e = body.lastIndexOf("]");
          String subStr = body.substring(b+1, e);
          System.out.println(subStr);
          Matcher matcher = pattern.matcher(subStr);
          while (matcher.find()){
            LhbItemJG lhbItemJG = new LhbItemJG();
            String group = matcher.group();
            System.out.println(group);
            String[] item = group.split(",");
            LhbItem lhbItem = new LhbItem();
            lhbItem.setTradeDate(item[0]);
            lhbItem.setCode(item[1]);
            lhbItem.setCodeName(item[2]);
            lhbItem.setComment(item[3]);
            lhbItem.setSign(item[4]);
            lhbItem.setClosePrice(item[5]);
            lhbItem.setZdf(item[6]);
            //获取机构交易数据
            List<LhbJGitem> jgList = new ArrayList<>();
            String lhbjgUrl = String.format(lhbJGUrl, new String[]{item[1].replace("\"",""),date,item[4].replace("\"","")});
            String jg = Jsoup.connect(lhbjgUrl).timeout(10000).ignoreContentType(true).execute().body();
            int jg_b = jg.indexOf("[");
            int jg_e = jg.lastIndexOf("]");
            String jgSub = jg.substring(jg_b+1, jg_e);
            System.out.println(jgSub);
            Matcher subMatch = pattern.matcher(jgSub);
            while (subMatch.find()){
              String subGroup = subMatch.group();
              System.out.println(subGroup);
              String[] subItem = subGroup.split(",");
              LhbJGitem lhbJGitem = new LhbJGitem();
              lhbJGitem.setCode(subItem[0]);
              lhbJGitem.setCodeName(subItem[1]);
              lhbJGitem.setType(subItem[2]);
              lhbJGitem.setRank(subItem[3]);
              lhbJGitem.setDate(subItem[4]);
              lhbJGitem.setJgname(subItem[5]);
              lhbJGitem.setBmount(subItem[6]);
              lhbJGitem.setSmount(subItem[7]);
              jgList.add(lhbJGitem);
            }
            lhbItemJG.setItem(lhbItem);
            lhbItemJG.setJgList(jgList);
            lhbList.add(lhbItemJG);
          }
          //写文件,过滤空文件
          if (lhbList != null && lhbList.size() > 0) {
            writeFile(lhbList,fileUri);
          }

        }catch (Exception e){
          e.printStackTrace();
          continue;//忽略出错日期
        }
      }
      if (lhbList != null && lhbList.size() > 0) {
        mapCol.put(date,lhbList);
      }

    }
    return mapCol;

  }

  public static List<LhbItemJG> loadFile(String fileUrl) {
    File fileInput = new File(fileUrl);
    List<LhbItemJG> list = null;
    FileInputStream fis = null;
    ObjectInputStream ois = null;
    try {
      fis = new FileInputStream(fileInput);
      ois = new ObjectInputStream(fis);
      list = (List<LhbItemJG>) ois.readObject();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (ois != null) {
          ois.close();
        }
        if (fis != null) {
          fis.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return list;
  }

  public static void writeFile(List<LhbItemJG> list, String fileUrl) {
    File file = new File(fileUrl);
    try {
      FileOutputStream fos = new FileOutputStream(file);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(list);
      oos.flush();
      oos.close();
      fos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  public static boolean fileExist(String fileUrl) {
    File file = new File(fileUrl);
    return file.exists();
  }


  public static Map<String,Set<String>> genKeySet(List<LhbItemJG> lists, Map<String,Set<String>> resMap, Predicate filter){
    for (LhbItemJG itemJG : lists) {
      LhbItem item = itemJG.getItem();
      List<LhbJGitem> jgList = itemJG.getJgList();
      String code = item.getCode();
      String tradeDate = item.getTradeDate();
      String key = code+"_"+tradeDate;
      Collection<LhbJGitem> jglistFilter = Collections2.filter(jgList, filter);
      Collection<String> jgCol =
          Collections2.transform(jglistFilter, new Function<LhbJGitem, String>() {
            @Override
            public String apply(LhbJGitem input) {
              return input.getJgname().replace("\"","") + "@" + input.getType().replace("\"","");
            }
          });
      HashSet<String> jgSet = Sets.newHashSet(jgCol);
      resMap.put(key,jgSet);
    }
    return resMap;
  }

  public static Map<String,String> setToMap(Set<String> set){
    Map<String, String> maps = new HashMap<String, String>();
    for (String item : set) {
      String[] array = item.split("@");
      maps.put(array[0],array[1]);
    }
    return maps;
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
    System.out.println(resPrint);
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
}
