package com.briup.bigdata.project.gres.step9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SaveRecommendResultToDB extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SaveRecommendResultToDB(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path("/user/liuze/step8/*");

        Job job=Job.getInstance(conf,"hdfs2db");
        job.setJarByClass(this.getClass());

        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        job.setMapOutputKeyClass(UgxWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);
        job.setOutputFormatClass(DBOutputFormat.class);

        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/hadoop",
                "root","root");
        DBOutputFormat.setOutput(job,"results","uid","gid","exp");
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(UgxWritable.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)?0:1;
    }
    public static class SaveRecommendResultToDBMapper
            extends Mapper<LongWritable, Text,UgxWritable, NullWritable>{
        private UgxWritable k2=new UgxWritable();
        private NullWritable v2=NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            this.k2.setUid(strs[0]);
            this.k2.setGid(strs[1]);
            this.k2.setExp(Integer.parseInt(strs[2]));
            context.write(this.k2,this.v2);
        }
    }
}
