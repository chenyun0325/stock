package datacrawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by cy111966 on 2016/11/15.
 */
public class HtmlAnalysis_V {

  public static final String dir = "D:/stock_data/holders";

  public static void main(String[] args) {
    String[] codes = {"600051"};
    String div_c = "bd_1";
    for (String code : codes) {
      String fileName = dir + "/" + code + ".html";
      analysisDoc(fileName, div_c);
    }
  }

  /**
   * 提取文件中指定区块
   */
  public static void analysisDoc(String url, String id) {
    File input = new File(url);
    try {
      Document doc = Jsoup.parse(input, "utf-8");
      Element div = doc.getElementById(id);//bd_1
      Elements tabs = div.select("a.fdates");
      List<String> div_c_set = new ArrayList<String>();
      for (Element tab : tabs) {
        String date = tab.text();
        System.out.println(date);
        String div_id = tab.attr("targ");
        ;
        System.out.println(div_id);
        div_c_set.add(div_id);
      }
      for (String div_sub : div_c_set) {
        System.err.println(div_sub);
        //analysisDoc_sub_head(div_sub, div, "table.ggintro", "thead");
        analysisDoc_sub_body(div_sub, div, "table.ggintro", "tr");
        System.err.println("-----out");
        //analysisDoc_sub_body(div_sub, div, "table.m_table", "tr.gray");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

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

  public static void analysisDoc_sub_body(String div_sub_body, Element div, String first_sel,
                                          String tr_sel) {
    Element div_ele = div.getElementById(div_sub_body);
    Elements bodys = div_ele.select(first_sel).select("tbody");
    System.out.println(bodys.size());
    for (Element body : bodys) {
      Elements trs = body.select(tr_sel);
      for (Element tr : trs) {
        //处理头
        Elements ths = tr.select("th");
        for (Element th : ths) {
          String text = th.text();
          System.out.println(text);
        }
        //处理其它
        Elements tds = tr.select("td");
        for (Element td : tds) {
          String text = td.text();
          System.out.println(text);
        }
        System.out.println("------------------body tr");
      }

    }
  }

}
