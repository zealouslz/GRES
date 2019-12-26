package com.briup.bigdata.project.gres.step2;

import com.briup.bigdata.project.gres.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class GoodsCooccurrenceList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceList(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        String in="/user/liuze/step1/*";
        String out="/user/liuze/step2";
        JobUtil.setConf(conf,this.getClass(),"GRESStep2",in,out);
        JobUtil.setMapper(GoodsCooccurrenceListMapper.class,Text.class,Text.class,TextInputFormat.class);
        JobUtil.setReducer(Reducer.class,Text.class,Text.class,TextOutputFormat.class,1);
        return JobUtil.commit();
    }
    public static class GoodsCooccurrenceListMapper
            extends Mapper<LongWritable, Text,Text,Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] strs = v1.toString().split("[\t]");
            String[] strs1 = strs[1].split("[,]");
            for (String s1 : strs1) {
                for (String s2 : strs1) {
                    this.k2.set(s1);
                    this.v2.set(s2);
                    context.write(this.k2,this.v2);
                }

            }
        }
    }
//    static class GoodsCooccurrenceListReducer
//        extends Reducer<Text,Text,Text,Text>{
//        private Text k3=new Text();
//        private Text v3=new Text();
//        @Override
//        protected void reduce(Text k2, Iterable<Text> v2s, Context context)
//                throws IOException, InterruptedException {
//            for (Text v21 : v2s) {
//                for (Text v22 : v2s) {
//                    this.k3.set(v21);
//                    this.v3.set(v22);
//                    context.write(this.k3,this.v3);
//                }
//            }
//        }
//    }
}
