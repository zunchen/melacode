package com.mela.mapred.job;

import com.mela.mapred.mapper.SourcePhoneDataDealMapper;
import com.mela.mapred.reducer.SourcePhoneDataDealReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author chenzun
 *
 * 原始数据预处理任务类：
 *      数据清洗、字段补全、合并
 *
 */
public class SourcePhoneDataDealJob {

    public static void main(String[] args) throws Exception {
        // 加载hadoop配置
        Configuration conf = new Configuration();

        // 校验命令行输入参数
        if (args.length < 2) {
            System.err.println("Usage: datadeal <in> [<in>...] <out>");
            System.exit(2);
        }

        // 构造一个Job实例job，并命名为"word count"
        Job job = Job.getInstance(conf, "Source Phone Data Deal");

        // 设置jar
        job.setJarByClass(SourcePhoneDataDealJob.class);

        /**
         * map端相关设置
         */
        job.setMapperClass(SourcePhoneDataDealMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Combiner
        job.setCombinerClass(SourcePhoneDataDealReducer.class);

        // 设置Reducer
        job.setReducerClass(SourcePhoneDataDealReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 添加输入路径（文件或目录），可以是多个
        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        // 添加输出路径
        FileOutputFormat.setOutputPath(job,
                new Path(args[args.length - 1]));

        // 等待作业job运行完成并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
