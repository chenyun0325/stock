package datacrawler;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;

/**
 * Created by cy111966 on 2016/11/15.
 */
public class JsonLoadEastmoney {

  public static void main(String[] args) {
    String url =
        "http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=GG&sty=GDRS&st=2&sr=-1&p=1&ps=50000&js={pages:(pc),data:[(x)]}&mkt=1&fd=2016-9-30&rt=49297871\n";
    try {
      String body = Jsoup.connect(url).timeout(5000).ignoreContentType(true).execute().body();
      System.out.println(body);
      JSONObject res = JSONObject.fromObject(body);
      JSONArray jsonArray = res.optJSONArray("data");
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
        Object bean = JSONObject.toBean(jsobj, EastMoneyHolder.class);
        System.out.println(bean);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
