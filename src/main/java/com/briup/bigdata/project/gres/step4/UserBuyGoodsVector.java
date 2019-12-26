package com.briup.bigdata.project.gres.step4;

import com.briup.bigdata.project.gres.JobUtil;
import com.briup.bigdata.project.gres.step3.GoodsCooccurrenceMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;


public class UserBuyGoodsVector extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsVector(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        String in="/user/liuze/step1/*";
        String out="/user/liuze/step4";
        JobUtil.setConf(conf,this.getClass(),"GRESStep4",in,out);
        JobUtil.setMapper(UserBuyGoodsVectorMapper.class,Text.class,Text.class,TextInputFormat.class);
        JobUtil.setReducer(UserBuyGoodsVectorReducer.class,Text.class,Text.class,TextOutputFormat.class,1);
        return JobUtil.commit();
    }
    public static class UserBuyGoodsVectorMapper
            extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context)
                throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            String[] strs1 = strs[1].toString().split("[,]");
            for (String s : strs1) {
                this.k2.set(s);
                this.v2.set(strs[0]);
                context.write(this.k2,this.v2);
            }
        }
    }
    public static class UserBuyGoodsVectorReducer
            extends Reducer<Text,Text,Text,Text>{
        private Text k3=new Text();
        private Text v3=new Text();
        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map=new TreeMap<>();
            for (Text v2 : v2s) {
                if(map.containsKey(v2.toString())){
                    int old = map.get(v2.toString());
                    map.put(v2.toString(),old+1);
                }else {
                    map.put(v2.toString(),1);
                }
            }
            this.k3.set(k2.toString());
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String result = entry.getKey() + ":" + entry.getValue();
                sb.append(result).append(",");
            }
            sb.setLength(sb.length()-1);
            this.v3.set(sb.toString());
            context.write(this.k3,this.v3);
        }
    }
}
