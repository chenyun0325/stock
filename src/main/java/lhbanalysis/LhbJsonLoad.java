package lhbanalysis;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;

/**
 * Created by cy111966 on 2016/12/19.
 */
public class LhbJsonLoad {

  public static void main(String[] args) {
    String url =
        "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_js?b=20161128&e=20161128";
    try {
      String body = Jsoup.connect(url).timeout(5000).ignoreContentType(true).execute().body();
      System.out.println(body);
      JSONObject res = JSONObject.fromObject(body);
      JSONArray jsonArray = res.optJSONArray("_datas");
      for (Object item : jsonArray) {
        System.out.println(item);
        System.err.println("-------------");
        String itemTrs = (String) item;
        String[] array = itemTrs.split(",");
        String[] arrayTrs = new String[array.length];
        for (int i= 0;i<array.length;i++) {
          arrayTrs[i]="\""+array[i]+"\"";
          System.out.println(arrayTrs[i]);
        }
        String json = String.format("{code:%s,codeName:%s,holderNum:%s,holderChange:%s,numPerHolder:%s,ltSdStockNum:%s,ltSdPer:%s,sdStockNum:%s,sdPer:%s,jgStockNum:%s,jgPer:%s,des:%s,x1:%s,x2:%s,date:%s,jc:%s}", arrayTrs);
        System.out.println(json);
        JSONObject jsobj = JSONObject.fromObject(json);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
