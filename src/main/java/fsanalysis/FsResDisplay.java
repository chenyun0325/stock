package fsanalysis;

import java.util.List;

/**
 * Created by cy111966 on 2016/11/25.
 */
public class FsResDisplay {
  private FsRes lj_item;
  private List<FsRes> resList;
  private String file_name;

  public String getFile_name() {
    return file_name;
  }

  public void setFile_name(String file_name) {
    this.file_name = file_name;
  }

  public List<FsRes> getResList() {
    return resList;
  }

  public void setResList(List<FsRes> resList) {
    this.resList = resList;
  }

  public FsRes getLj_item() {
    return lj_item;
  }

  public void setLj_item(FsRes lj_item) {
    this.lj_item = lj_item;
  }
}
