package com.briup.bigdata.project.gres.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DuplicateDataForResult extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateDataForResult(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in1 =new Path("/user/liuze/step1/*");
        Path in2=new Path("/user/liuze/step6/*");
        Path out=new Path("/user/liuze/step7");
        Job job=Job.getInstance(conf,"GRESStep7");//值复制
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,in1, TextInputFormat.class, DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class, DuplicateDataForResultSecondMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
    public static class DuplicateDataForResultFirstMapper
            extends Mapper<LongWritable, Text,Text, IntWritable>{
        private Text k2=new Text();
        private IntWritable v2=new IntWritable(0);

        @Override
        protected void map(LongWritable k1, Text v1, Context context)
                throws IOException, InterruptedException {
            String[] strs1 = v1.toString().split("[\t]");
            String[] strs2 = strs1[1].split("[,]");
            for (String s : strs2) {
                String res = strs1[0]+"\t"+s;
                this.k2.set(res);
                context.write(this.k2,this.v2);
            }
        }
    }
    public static class DuplicateDataForResultSecondMapper
            extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text k2=new Text();
        private IntWritable v2=new IntWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            String[] strs1 = strs[0].split("[,]");
            String res=strs1[0]+"\t"+strs1[1];
            this.k2.set(res);
            this.v2.set(Integer.parseInt(strs[1]));
            context.write(this.k2,this.v2);
        }
    }
    public static class DuplicateDataForResultReducer
            extends Reducer<Text,IntWritable,Text,IntWritable>{
          private Text k3=new Text();
          private IntWritable v3=new IntWritable();

        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
            List<Integer> list=new ArrayList<>();
            this.k3.set(k2.toString());

                for (IntWritable v2 : v2s) {
                   list.add(v2.get());
                }
            if(list.size()==1){
                this.v3.set(list.get(0));
                context.write(this.k3,this.v3);
            }
        }
    }
}
