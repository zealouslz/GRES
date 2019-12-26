package com.briup.bigdata.project.gres.step8;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DataSort  extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
       System.exit(ToolRunner.run(new DataSort(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path("/user/liuze/step7/*");
        Path out=new Path("/user/liuze/step8");

        Job job=Job.getInstance(conf,"数据排序");
        job.setJarByClass(this.getClass());

        job.setMapperClass(DataSortMapper.class);
        job.setMapOutputKeyClass(DataSortWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(DataSortWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }
    public static class DataSortMapper extends Mapper<LongWritable, Text,DataSortWritable,NullWritable>{
        private DataSortWritable k2=new DataSortWritable();
        private NullWritable v2=NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.setUid(strs[0]);
            this.k2.setGid(strs[1]);
            this.k2.setExp(strs[2]);
            context.write(this.k2,this.v2);
        }
    }
}
