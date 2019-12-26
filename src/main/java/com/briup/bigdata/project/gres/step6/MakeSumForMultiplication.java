package com.briup.bigdata.project.gres.step6;

import com.briup.bigdata.project.gres.JobUtil;
import com.briup.bigdata.project.gres.step4.UserBuyGoodsVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MakeSumForMultiplication extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MakeSumForMultiplication(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        String in="/user/liuze/step5/*";
        String out="/user/liuze/step6";
        JobUtil.setConf(conf,this.getClass(),"GRESStep5",in,out);
        JobUtil.setMapper(MakeSumForMultiplicationMapper.class,Text.class,IntWritable.class, TextInputFormat.class);
        JobUtil.setCombiner(true,MakeSumForMultiplicationReducer.class);
        JobUtil.setReducer(MakeSumForMultiplicationReducer.class,Text.class,IntWritable.class, TextOutputFormat.class,1);
        return JobUtil.commit();
    }
    public static class MakeSumForMultiplicationMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        private Text k2=new Text();
        private IntWritable v2=new IntWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.set(strs[0]);
            this.v2.set(Integer.parseInt(strs[1]));
            context.write(this.k2,this.v2);
        }
    }
    public static class MakeSumForMultiplicationReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private Text k3=new Text();
        private IntWritable v3=new IntWritable();
        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context)
                throws IOException, InterruptedException {
            int sum=0;
            this.k3.set(k2.toString());
            for (IntWritable v2 : v2s) {
                sum+=v2.get();
            }
            this.v3.set(sum);
            context.write(this.k3,this.v3);
        }
    }
}
