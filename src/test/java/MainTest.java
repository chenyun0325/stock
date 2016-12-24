import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by cy111966 on 2016/12/21.
 */
public class MainTest {

  public static void main(String[] args) {
    FileWriter fw = null;
    BufferedWriter bfw = null;
    try {
      fw = new FileWriter("D:/stock_data/xyz.tttttt", true);
      bfw = new BufferedWriter(fw);
      Long time = System.currentTimeMillis();
      bfw.write(time.toString());
      bfw.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        bfw.close();
        fw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}
