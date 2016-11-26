package datacrawler;

import net.sf.json.JSONArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by cy111966 on 2016/11/17.
 */
public class Test {
  static Logger log = LoggerFactory.getLogger(Test.class);
  public static void main(String[] args) {

    Set<Integer> result = new HashSet<Integer>();
    Set<Integer> set1 = new HashSet<Integer>(){{
      add(1);
      add(3);
      add(5);
    }};

    Set<Integer> set2 = new HashSet<Integer>(){{
      add(11);
      add(2);
      add(3);
    }};

    Set<Integer> set3 = new HashSet<Integer>(){{
      add(1);
      add(2);
      add(3);
    }};

    result.clear();
    result.addAll(set1);
    result.retainAll(set2);
    System.out.println("交集："+result);

    result.clear();
    result.addAll(set1);
    result.retainAll(set3);
    System.out.println("交集："+result);

    System.out.println(JSONArray.fromObject(set1).toString());

    String[] stocks = Constant.stock_all.split(",");
    System.err.println(stocks.length);
    log.error("xxxxxxxxxxxxxxxxxxxx");
    SdLtHolderPerTimeContain contain = new SdLtHolderPerTimeContain();
    System.out.println(contain);
    String x="xxy\"adc\"kkk";
    System.out.println(x);
    String y = x.replaceAll("\"", "");
    System.out.println(y);
  }
}
