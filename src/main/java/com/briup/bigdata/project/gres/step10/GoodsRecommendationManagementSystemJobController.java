package com.briup.bigdata.project.gres.step10;

import com.briup.bigdata.project.gres.step1.UserBuyGoodsList;
import com.briup.bigdata.project.gres.step2.GoodsCooccurrenceList;
import com.briup.bigdata.project.gres.step3.GoodsCooccurrenceMatrix;
import com.briup.bigdata.project.gres.step4.UserBuyGoodsVector;
import com.briup.bigdata.project.gres.step5.MultiplyGoodsMatrixAndUserVector;
import com.briup.bigdata.project.gres.step6.MakeSumForMultiplication;
import com.briup.bigdata.project.gres.step7.DuplicateDataForResult;
import com.briup.bigdata.project.gres.step8.DataSort;
import com.briup.bigdata.project.gres.step8.DataSortWritable;
import com.briup.bigdata.project.gres.step9.SaveRecommendResultToDB;
import com.briup.bigdata.project.gres.step9.UgxWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GoodsRecommendationManagementSystemJobController {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "GRESStep1");
        Job job2 = Job.getInstance(conf, "GRESStep2");
        Job job3 = Job.getInstance(conf, "GRESStep3");
        Job job4 = Job.getInstance(conf, "GRESStep4");
        Job job5 = Job.getInstance(conf, "GRESStep5");
        Job job6 = Job.getInstance(conf, "GRESStep6");
        Job job7 = Job.getInstance(conf, "GRESStep7");
        Job job8 = Job.getInstance(conf, "GRESStep8");
        Job job9 = Job.getInstance(conf, "GRESStep9");
        //job1作业配置
        job1.setJarByClass(UserBuyGoodsList.class);
        job1.setMapperClass(UserBuyGoodsList.UserBuyGoodsListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1, new Path("/user/liuze/gres/matrix.txt"));
        job1.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("/user/liuze/step1"));
        //job2作业配置
        job2.setJarByClass(GoodsCooccurrenceList.class);
        job2.setMapperClass(GoodsCooccurrenceList.GoodsCooccurrenceListMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2, new Path("/user/liuze/step1/*"));
        job2.setReducerClass(Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("/user/liuze/step2"));
        //job3作业配置
        job3.setJarByClass(GoodsCooccurrenceMatrix.class);
        job3.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3, new Path("/user/liuze/step2/*"));
        job3.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3, new Path("/user/liuze/step3"));
        //job4作业配置
        job4.setJarByClass(UserBuyGoodsVector.class);
        job4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4, new Path("/user/liuze/step1/*"));
        job4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4, new Path("/user/liuze/step4"));
        //job5作业配置
        job5.setJarByClass(MultiplyGoodsMatrixAndUserVector.class);
        MultipleInputs.addInputPath(job5, new Path("/user/liuze/step3/*"), TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorMapper1.class);
        MultipleInputs.addInputPath(job5, new Path("/user/liuze/step4/*"), TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorMapper2.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5, new Path("/user/liuze/step5"));
        //job6作业配置
        job6.setJarByClass(MakeSumForMultiplication.class);
        job6.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job6, new Path("/user/liuze/step5/*"));
        job6.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6, new Path("/user/liuze/step6"));
        //job7作业配置
        job7.setJarByClass(DuplicateDataForResult.class);
        MultipleInputs.addInputPath(job7, new Path("/user/liuze/step1/*"), TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job7, new Path("/user/liuze/step6/*"), TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(IntWritable.class);
        job7.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(IntWritable.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7, new Path("/user/liuze/step7"));
        //job8作业配置
        job8.setJarByClass(DataSort.class);
        job8.setMapperClass(DataSort.DataSortMapper.class);
        job8.setMapOutputKeyClass(DataSortWritable.class);
        job8.setMapOutputValueClass(NullWritable.class);
        job8.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job8, new Path("/user/liuze/step7/*"));
        job8.setOutputKeyClass(DataSortWritable.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job8, new Path("/user/liuze/step8"));
        //job9作业配置
        job9.setJarByClass(SaveRecommendResultToDB.class);
        job9.setMapperClass(SaveRecommendResultToDB.SaveRecommendResultToDBMapper.class);
        job9.setMapOutputKeyClass(UgxWritable.class);
        job9.setMapOutputValueClass(NullWritable.class);
        job9.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job9, new Path("/user/liuze/step8/*"));
        job9.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(job9.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/hadoop",
                "root", "root");
        DBOutputFormat.setOutput(job9, "results", "uid", "gid", "exp");
        job9.setReducerClass(Reducer.class);
        job9.setOutputKeyClass(UgxWritable.class);
        job9.setOutputValueClass(NullWritable.class);

        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);
        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);
        ControlledJob cj3 = new ControlledJob(conf);
        cj3.setJob(job3);
        ControlledJob cj4 = new ControlledJob(conf);
        cj4.setJob(job4);
        ControlledJob cj5 = new ControlledJob(conf);
        cj5.setJob(job5);
        ControlledJob cj6 = new ControlledJob(conf);
        cj6.setJob(job6);
        ControlledJob cj7 = new ControlledJob(conf);
        cj7.setJob(job7);
        ControlledJob cj8 = new ControlledJob(conf);
        cj8.setJob(job8);
        ControlledJob cj9 = new ControlledJob(conf);
        cj9.setJob(job9);

        cj2.addDependingJob(cj1);
        cj3.addDependingJob(cj2);
        cj4.addDependingJob(cj1);
        cj5.addDependingJob(cj3);
        cj5.addDependingJob(cj4);
        cj6.addDependingJob(cj5);
        cj7.addDependingJob(cj1);
        cj7.addDependingJob(cj6);
        cj8.addDependingJob(cj7);
        cj9.addDependingJob(cj8);

        JobControl jc = new JobControl("GRES");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);
        jc.addJob(cj8);
        jc.addJob(cj9);
        Thread jcThread = new Thread(jc);
        jcThread.start();
        if (jc.allFinished()) {
            System.out.println("jc执行完毕");
            System.out.println(jc.getSuccessfulJobList());
            jc.stop();
        }
        if (jc.getFailedJobList().size() > 0) {
            System.out.println("jc执行失败列表：");
            System.out.println(jc.getFailedJobList());
            jc.stop();
        }
    }
}
