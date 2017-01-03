package stormpython;

import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import fsrealanalysis.FsData;
import fsrealanalysis.FsIndexRes;
import fsrealanalysis.SingleFsDataProcess;

/**
 * Created by chenyun on 16/2/2.
 */
public class Bolt1 extends BaseBasicBolt {

    Map<String ,List<FsData>> code_map = new ConcurrentHashMap<String, List<FsData>>();//存储股票分时数据

    Map<String ,List<FsIndexRes>> code_yd_map = new ConcurrentHashMap<String, List<FsIndexRes>>();//存储股票压单数据

    Map<String ,List<FsIndexRes>> code_td_map = new ConcurrentHashMap<String, List<FsIndexRes>>();//存储股票托单数据

    Map<String ,Deque<FsIndexRes>> code_wid_map = new ConcurrentHashMap<String, Deque<FsIndexRes>>();

     SingleFsDataProcess process = new SingleFsDataProcess();//分时数据处理函数

    private double filter_per ;//压单或者托单比例

    private double filter_mount;//压单或者托单金额

    private int max_size;//最大大小

    public Bolt1(double filter_mount, double filter_per,int max_size) {
        this.filter_mount = filter_mount;
        this.filter_per = filter_per;
        this.max_size = max_size;
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String code = input.getString(0);
            Object item = input.getValue(1);
            JSONObject item_json = JSONObject.fromObject(item);
            FsData fsdata = (FsData) JSONObject.toBean(item_json, FsData.class);
            code_map = transfer(code_map,code,fsdata);
            //处理单个数据----每新增1个数据分析一次
            FsIndexRes indexRes = this.process.process(fsdata);
            boolean b_ge_s = indexRes.isB_ge_s();
            double per = indexRes.getPer();
            double a_all_m = indexRes.getA_all_m();//可以利用平均值
            double b_all_m = indexRes.getB_all_m();
            if (b_ge_s) {
               if (per>filter_per&&b_all_m>filter_mount){//托单
                   code_td_map = transferRes(code_td_map,code,indexRes);
               }
            }else {
                if (per>filter_per&&a_all_m>filter_mount){//压单
                    code_yd_map = transferRes(code_yd_map,code,indexRes);
                }
            }

            // TODO: 2016/12/3 下一个bolt进行时序数据分析
            //夹单条件:a1_p>0.9&b1_p>0.9&jd_per=1??
            //处理时间序列数据----每新增n个数据分析一次
            Deque<FsIndexRes> fsDatas = code_wid_map.get(code);
            if (fsDatas != null) {
                int size = fsDatas.size();
                fsDatas.offerLast(indexRes);
                if (size>=max_size){
                    fsDatas.pollFirst();
                }
            }else {
                Deque<FsIndexRes> fs_list = new LinkedList<>();
                fs_list.offerLast(indexRes);
                code_wid_map.put(code,fs_list);
            }
            //code = code + "bolt1";
            MessageId messageId = input.getMessageId();
            System.err.println(messageId);
            System.err.println("对消息加工第1次-------[arg0]:" + code  + "---[arg2]:" + item + "------->" + code);
            if (code != null) {
                collector.emit(new Values(code,code_map.get(code)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("code","list"));
    }

    public Map<String,List<FsData>> transfer(Map<String,List<FsData>> map,String code,FsData item){
        List<FsData> fsDatas = map.get(code);
        if (fsDatas != null) {
            fsDatas.add(item);
        }else {
            List<FsData> fs_list = new ArrayList<FsData>();
            fs_list.add(item);
            map.put(code,fs_list);
        }
        return map;
    }
    public Map<String,List<FsIndexRes>> transferRes(Map<String,List<FsIndexRes>> map,String code,FsIndexRes item){

        List<FsIndexRes> fsDatas = map.get(code);
        if (fsDatas != null) {
            fsDatas.add(item);
        }else {
            List<FsIndexRes> fs_list = new ArrayList<FsIndexRes>();
            fs_list.add(item);
            map.put(code,fs_list);
        }
        return map;
    }
}
