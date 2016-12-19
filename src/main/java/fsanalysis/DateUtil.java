package fsanalysis;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by cy111966 on 2016/11/25.
 */
public class DateUtil {
  public static String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static String TIME_FORMAT_X="yyyyMMdd";
  public static long convert2long(String date, String format) {
    try {
      if (StringUtils.isNotBlank(date)) {
        if (StringUtils.isBlank(format)) {
          format = TIME_FORMAT;
        }
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.parse(date).getTime();
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return 0l;
  }

  public static Date convert2date(long timelong){
    //SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
    Date date = new Date(timelong);
    return date;
  }

  public static String convert2dateStr(long timelong){
    SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
    Date date = new Date(timelong);
   return sdf.format(date);
  }

  public static Date convert2date(String time)  {
    SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_X);
    Date date = null;
    try {
      date = sdf.parse(time);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return date;
  }

  public static String convert2dateStr(Date time)  {
    SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_X);
    String dateStr = sdf.format(time);
    return dateStr;
  }

  public static Date date_computer(Date date,int period,Unit unit){
    //Calendar instance = Calendar.getInstance();
    GregorianCalendar instance = new GregorianCalendar();
    instance.setTime(date);
    switch (unit){
      case min:
        instance.add(Calendar.MINUTE,period);
        break;
      case hour:
        instance.add(Calendar.HOUR,period);
        break;
      case day:
        instance.add(Calendar.DATE,period);
        break;
    }
    return instance.getTime();
  }


}
