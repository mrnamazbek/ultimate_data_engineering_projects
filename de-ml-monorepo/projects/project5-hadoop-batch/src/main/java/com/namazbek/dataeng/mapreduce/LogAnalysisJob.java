package com.namazbek.dataeng.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hadoop MapReduce job for analyzing application logs.
 * Counts ERROR and WARN messages by component and hour.
 */
public class LogAnalysisJob {

    public static class LogAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        // Log pattern: timestamp [LEVEL] Component: message - RequestID
        private final Pattern LOG_PATTERN = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2} \\d{2}):\\d{2}:\\d{2} \\[(ERROR|WARN|INFO|DEBUG)\\] ([^:]+):.*"
        );

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            Matcher matcher = LOG_PATTERN.matcher(line);
            
            if (matcher.matches()) {
                String hourTimestamp = matcher.group(1); // YYYY-MM-DD HH
                String level = matcher.group(2);
                String component = matcher.group(3).trim();
                
                // Only process ERROR and WARN messages
                if ("ERROR".equals(level) || "WARN".equals(level)) {
                    // Emit: "hour:level:component" -> 1
                    String compositeKey = hourTimestamp + ":" + level + ":" + component;
                    word.set(compositeKey);
                    context.write(word, one);
                }
            }
        }
    }

    public static class LogAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LogAnalysisJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        // Set job configuration
        conf.set("mapreduce.job.name", "Log Analysis Job");
        conf.set("mapreduce.job.queuename", "default");
        
        Job job = Job.getInstance(conf, "log analysis");
        job.setJarByClass(LogAnalysisJob.class);
        job.setMapperClass(LogAnalysisMapper.class);
        job.setCombinerClass(LogAnalysisReducer.class);
        job.setReducerClass(LogAnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
