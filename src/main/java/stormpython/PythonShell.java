package stormpython;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by cy111966 on 2016/12/27.
 */
public class PythonShell {

  public static void main(String[] args){
    try {
      //todo 路径作为参数传递进来
      String path = PythonShell.class.getResource("").getPath();
      String absolutePath = new File("").getAbsolutePath();
      System.out.println(absolutePath);
      System.out.println(path);
      String s ="python "+ path + "fsdata_load.py"+" 000798 2016-12-25 2016-12-27";
      System.out.println(s);
      String urlPath = "D:\\alibaba-workspace\\ideaWorkspace\\stockanalysis\\src\\main\\java\\stormpython\\fsdata_load.py";
      String cmd="python fsdata_load.py 000798 2016-12-25 2016-12-27";
      System.out.println(cmd);
      Runtime rt = Runtime.getRuntime();
      String command="python "+urlPath+" 000798 2016-12-25 2016-12-27";
      Process pcs = rt.exec(command);
      InputStream pcsInputStream = pcs.getInputStream();
      InputStream pcsErrorStream = pcs.getErrorStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(pcsInputStream));
      String line;
      while ((line =br.readLine())!=null){
        System.out.println(line);
      }
      BufferedReader brError = new BufferedReader(new InputStreamReader(pcsErrorStream));
      while ((line =brError.readLine())!=null){
        System.out.println(line);
      }
      try {
        pcs.waitFor();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      br.close();
      int ret = pcs.exitValue();
      System.out.println(ret);
      System.out.println("执行完毕");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
