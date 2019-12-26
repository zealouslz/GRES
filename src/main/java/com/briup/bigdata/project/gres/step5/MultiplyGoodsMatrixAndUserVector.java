package com.briup.bigdata.project.gres.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.util.Map;
import java.util.TreeMap;

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in1 =new Path("/user/liuze/step3/*");
        Path in2=new Path("/user/liuze/step4/*");
        Path out=new Path("/user/liuze/step5");
        Job job=Job.getInstance(conf,"GRESStep5");//值复制
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,in1, TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorMapper1.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorMapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
    public static class MultiplyGoodsMatrixAndUserVectorMapper1
            extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[\t]");
            String[] strs1 = strs[1].split("[,]");
            this.k2.set(strs[0]);
            for (String s : strs1) {
                this.v2.set(s);
                context.write(this.k2,this.v2);
            }

        }
    }
    public static class MultiplyGoodsMatrixAndUserVectorMapper2
            extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

            String[] strs = v1.toString().split("[\t]");
            String[] strs1 = strs[1].split("[,]");
            this.k2.set(strs[0]);
            for (String s : strs1) {
                this.v2.set(s);
                context.write(this.k2,this.v2);
            }
        }
    }
    public static class MultiplyGoodsMatrixAndUserVectorReducer
            extends Reducer<Text,Text,Text,Text>{
        private Text k3 = new Text();
        private Text v3=new Text();
        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context)
                throws IOException, InterruptedException {
            Map<String,Integer> map1=new TreeMap<>();
            Map<String,Integer> map2=new TreeMap<>();
            for (Text v2 : v2s) {
                String[] strs = v2.toString().split("[:]");
                if("1".equals(strs[0].substring(0,1))){
                    map1.put(strs[0],Integer.parseInt(strs[1]));
                }
                else if("2".equals(strs[0].substring(0,1))){
                    map2.put(strs[0],Integer.parseInt(strs[1]));
                }
            }
            for (Map.Entry<String, Integer> entry1 : map1.entrySet()) {
                for (Map.Entry<String, Integer> entry2 : map2.entrySet()) {
                    String s=entry1.getKey()+","+entry2.getKey();
                    Integer res=entry1.getValue()*entry2.getValue();
                    this.k3.set(s);
                    this.v3.set(res.toString());
                    context.write(this.k3,this.v3);
                }
            }
        }
    }
}
