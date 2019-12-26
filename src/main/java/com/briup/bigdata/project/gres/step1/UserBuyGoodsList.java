package com.briup.bigdata.project.gres.step1;

import com.briup.bigdata.project.gres.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class UserBuyGoodsList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsList(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        String in="/user/liuze/gres/matrix.txt";
        String out="/user/liuze/step1";
        JobUtil.setConf(conf,this.getClass(),"GRESStep1",in,out);
        JobUtil.setMapper(UserBuyGoodsListMapper.class,Text.class,Text.class,TextInputFormat.class);
        JobUtil.setReducer(UserBuyGoodsListReducer.class,Text.class,Text.class,TextOutputFormat.class,1);
        return JobUtil.commit();
    }
    public static class UserBuyGoodsListMapper
            extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split(" ");
            this.k2.set(strs[0]);
            this.v2.set(strs[1]);
            context.write(this.k2,this.v2);
        }
    }
    public static class UserBuyGoodsListReducer
            extends Reducer<Text,Text,Text,Text>{
        private Text k3=new Text();
        private Text v3=new Text();
        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            this.k3.set(k2.toString());
            StringBuilder sb=new StringBuilder();
            for (Text v2 : v2s) {
                sb.append(v2.toString()).append(",");
            }
           sb.setLength(sb.length()-1);
            this.v3.set(sb.toString());
            context.write(this.k3,this.v3);
        }
    }
}
