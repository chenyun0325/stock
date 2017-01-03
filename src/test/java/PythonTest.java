import org.python.core.Py;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

/**
 * Created by cy111966 on 2016/12/26.
 */
public class PythonTest {

  public static void main(String[] args) {
    try {
      PythonInterpreter interpreter = new PythonInterpreter();
      PySystemState sys = Py.getSystemState();
      sys.path.add("D:\\program\\Anaconda2\\Lib\\xml");
      sys.path.add("D:\\program\\Anaconda2\\Lib\\site-packages");
      interpreter.exec("import sys");
      interpreter.exec("print sys.path");
      String basePath = PythonTest.class.getResource("").getPath();
      interpreter.execfile("fsdata_load.py");
      PyFunction fs_load = interpreter.get("fs_load", PyFunction.class);

     PyObject obj = fs_load.__call__(new PyString("000798"), new PyString("2016-12-07"));
      //PyObject obj = fs_load.__call__(Py.newStringOrUnicode("000798"), Py.newStringOrUnicode("2016-12-07"));
      System.out.println(obj);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
