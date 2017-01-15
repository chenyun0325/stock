package db;

import com.dangdang.ddframe.rdb.sharding.api.ShardingDataSourceFactory;
import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.rule.BindingTableRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.NoneDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.SingleKeyTableShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.google.common.collect.Lists;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.zip.CRC32;

import javax.sql.DataSource;

/**
 * http://blog.csdn.net/clypm/article/details/54378523
 * http://blog.csdn.net/clypm/article/details/54378502
 * Created by cy111966 on 2017/1/12.
 */
public class ShardingJDBC {

  public static void main(String[] args) {
    /**
     *
     * ShardingRule shardingRule = ShardingRule.builder()
     .dataSourceRule(dataSourceRule)
     .tableRules(tableRuleList)
     .databaseShardingStrategy(new DatabaseShardingStrategy("sharding_column", new XXXShardingAlgorithm()))
     .tableShardingStrategy(new TableShardingStrategy("sharding_column", new XXXShardingAlgorithm())))
     .build();
     */
    //数据源
    try {
    Map<String,DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put("db_test",createDataSource("db_test","root","123456"));
    DataSourceRule dataSourceRule = new DataSourceRule(dataSourceMap);
    //分库分表的表,第一个参数是逻辑表名,第二个是实际表名，第三个是实际库
    TableRule fs_his_tr =
        TableRule.builder("fs_st_his_data").dataSourceRule(dataSourceRule)
            .actualTables(Arrays.asList("fs_st_his_data_0", "fs_st_his_data_1")).build();

    TableRule fs_pk_tr =
        TableRule.builder("fs_st_pk_data").dataSourceRule(dataSourceRule)
            .actualTables(Arrays.asList("fs_st_pk_data_0", "fs_st_pk_data_1")).build();

    ShardingRule shardingRule = ShardingRule.builder()
        .dataSourceRule(dataSourceRule)
        .tableRules(Lists.newArrayList(fs_pk_tr,fs_his_tr))
        //.bindingTableRules(Lists.newArrayList(new BindingTableRule(Lists.newArrayList(fs_pk_tr,fs_his_tr))))
        //.databaseShardingStrategy(new DatabaseShardingStrategy("none",new NoneDatabaseShardingAlgorithm()))
        .tableShardingStrategy(new TableShardingStrategy("code", new StockFsSingleKeyTableShardingAlgorithm())).build();
    //创建ds
    DataSource ds = ShardingDataSourceFactory.createDataSource(shardingRule);

      String sql ="INSERT INTO `fs_st_pk_data`\n"
                  + "(`index`,\n"
                  + "`code`,\n"
                  + "`date`,\n"
                  + "`time`,\n"
                  + "`price`,\n"
                  + "`change`,\n"
                  + "`volume`,\n"
                  + "`amount`,\n"
                  + "`type`)\n"
                  + "VALUES\n"
                  + "('0', '12767', '2016-07-07', '15:00:03', '21.18', '-0.01', '1291', '2734338', '卖盘')";
      Connection con = ds.getConnection();
      con.setAutoCommit(false);
      PreparedStatement pstmt = con.prepareStatement(sql);
      int size = pstmt.executeUpdate();
      con.commit();
      System.out.println(size);
    } catch (Exception e) {
      e.printStackTrace();
    }


  }

  public static DataSource createDataSource(String dataSourceName,String userName,String psw){
    BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
    ds.setUrl(String.format("jdbc:mysql://localhost:3306/%s?useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true",dataSourceName));
    ds.setUsername(userName);
    ds.setPassword(psw);
    return ds;
  }

  static class StockFsSingleKeyTableShardingAlgorithm implements SingleKeyTableShardingAlgorithm<String>{

    int tableSize =2;
    @Override
    public String doEqualSharding(Collection<String> tableNames,
                                  ShardingValue<String> shardingValue) {
      String value = shardingValue.getValue();
      int vHash = newCompatHashingAlg(value);
      System.err.println(vHash);
      System.err.println(vHash % tableSize);
      for (String tableName : tableNames) {
        if (tableName.endsWith(vHash % tableSize + "")) {
          return tableName;
        }
      }
      throw new IllegalArgumentException();
    }

    @Override
    public Collection<String> doInSharding(Collection<String> tableNames,
                                           ShardingValue<String> shardingValue) {
      Collection<String> tablesRes = new HashSet<>();
      Collection<String> values = shardingValue.getValues();
      for (String value : values) {
        int vHash = newCompatHashingAlg(value);
        for (String tableName : tableNames) {
          if (tableName.endsWith(vHash % tableSize + "")) {
            tablesRes.add(tableName);
          }
        }
      }
      return tablesRes;
    }

    @Override
    public Collection<String> doBetweenSharding(Collection<String> tableNames,
                                                ShardingValue<String> shardingValue) {
      return null;
    }
    private static int newCompatHashingAlg( String key ) {
      CRC32 checksum = new CRC32();
      checksum.update( key.getBytes() );
      int crc = (int) checksum.getValue();
      return (crc >> 16) & 0x7fff;
    }
  }

}
