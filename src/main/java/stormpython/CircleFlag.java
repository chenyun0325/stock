package stormpython;

/**
 * Created by cy111966 on 2016/11/30.
 */
public class CircleFlag {

  public static void main(String[] args) throws InterruptedException {
    boolean flag = true;
    while (flag){//单线程
      flag=false;
      System.out.println(flag);
      System.out.println(11);
      Thread.sleep(100);
      flag=true;
      System.out.println(flag);
    }
  }

}
