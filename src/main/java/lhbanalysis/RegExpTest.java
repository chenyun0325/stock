package lhbanalysis;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by cy111966 on 2016/12/19.
 */
public class RegExpTest {

  public static void main(String[] args) {
    ArrayList<String> ls = new ArrayList<>();
    Pattern pattern = Pattern.compile("(?<=\\[)(.+?)(?=\\])");
    String str="[[\"2016-12-16\",\"000935\",\"四川双马\",\"日价格涨幅偏离值达到9.05%\",\"070001\",\"31.60\",\"10.00\"],[\"2016-12-16\",\"002819\",\"东方中科\",\"日换手率达到35.91%\",\"070004\",\"51.90\",\"-8.15\"]]";
    int begin = str.indexOf("[");
    int end = str.lastIndexOf("]");
    String subStr = str.substring(begin+1, end);
    System.out.println(subStr);
    Matcher matcher = pattern.matcher(subStr);
    while (matcher.find()){
      String group = matcher.group();
      System.out.println(group);
      ls.add(group);
    }
    System.out.println(ls);
  }

}
