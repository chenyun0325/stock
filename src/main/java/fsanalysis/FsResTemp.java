package fsanalysis;

/**
 * Created by cy111966 on 2016/11/25.
 */
public class FsResTemp {

  private long volume_sum;
  private long amount_sum;
  private long amount_var_sum;

  public long getAmount_sum() {
    return amount_sum;
  }

  public void setAmount_sum(long amount_sum) {
    this.amount_sum = amount_sum;
  }

  public long getAmount_var_sum() {
    return amount_var_sum;
  }

  public void setAmount_var_sum(long amount_var_sum) {
    this.amount_var_sum = amount_var_sum;
  }

  public long getVolume_sum() {
    return volume_sum;
  }

  public void setVolume_sum(long volume_sum) {
    this.volume_sum = volume_sum;
  }
}
