package db;

import com.dangdang.ddframe.rdb.sharding.api.ShardingDataSourceFactory;
import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.NoneDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.SingleKeyTableShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.google.common.collect.Lists;

import org.apache.commons.dbcp.BasicDataSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
    Map<String,DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put("sharding_0",createDataSource("dbtest","root","123456"));
    DataSourceRule dataSourceRule = new DataSourceRule(dataSourceMap);
    //分库分表的表,第一个参数是逻辑表名,第二个是实际表名，第三个是实际库
    TableRule fs_tr =
        TableRule.builder("fs_st_data").dataSourceRule(dataSourceRule)
            .actualTables(Arrays.asList("fs_st_data_0", "fs_st_data_1")).build();

    TableRule fs_pk_tr =
        TableRule.builder("fs_st_pk_data").dataSourceRule(dataSourceRule)
            .actualTables(Arrays.asList("fs_st_pk_data_0", "fs_st_pk_data_1")).build();

    ShardingRule shardingRule = ShardingRule.builder()
        .dataSourceRule(dataSourceRule)
        .tableRules(Lists.newArrayList(fs_pk_tr,fs_tr))
        .databaseShardingStrategy(new DatabaseShardingStrategy("none",new NoneDatabaseShardingAlgorithm()))
        .tableShardingStrategy(new TableShardingStrategy("code", new StockFsSingleKeyTableShardingAlgorithm())).build();
    //创建ds
    DataSource ds = ShardingDataSourceFactory.createDataSource(shardingRule);


  }

  public static DataSource createDataSource(String dataSourceName,String userName,String psw){
    BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
    ds.setUrl(String.format("jdbc:mysql://localhost:3306/%s?useUnicode=true&characterEncoding=utf-8",dataSourceName));
    ds.setUsername(userName);
    ds.setPassword(psw);
    return ds;
  }

  static class StockFsSingleKeyTableShardingAlgorithm implements SingleKeyTableShardingAlgorithm<String>{

    @Override
    public String doEqualSharding(Collection<String> availableTargetNames,
                                  ShardingValue<String> shardingValue) {
      return null;
    }

    @Override
    public Collection<String> doInSharding(Collection<String> availableTargetNames,
                                           ShardingValue<String> shardingValue) {
      return null;
    }

    @Override
    public Collection<String> doBetweenSharding(Collection<String> availableTargetNames,
                                                ShardingValue<String> shardingValue) {
      return null;
    }
  }

}
