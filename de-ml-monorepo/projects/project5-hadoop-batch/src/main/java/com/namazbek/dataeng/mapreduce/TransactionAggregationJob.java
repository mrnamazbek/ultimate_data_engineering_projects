package com.namazbek.dataeng.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Hadoop MapReduce job for aggregating transaction data.
 * Calculates daily revenue and transaction counts by country and transaction type.
 */
public class TransactionAggregationJob {

    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private ObjectMapper mapper = new ObjectMapper();
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            try {
                // Parse JSON transaction record
                JsonNode transaction = mapper.readTree(value.toString());
                
                // Extract fields
                String country = transaction.get("country").asText();
                String transactionType = transaction.get("transaction_type").asText();
                String status = transaction.get("status").asText();
                double amount = transaction.get("amount").asDouble();
                String timestamp = transaction.get("timestamp").asText();
                
                // Extract date from timestamp (YYYY-MM-DD)
                String date = timestamp.split(" ")[0];
                
                // Create composite key: date:country:transaction_type
                String compositeKey = date + ":" + country + ":" + transactionType;
                outputKey.set(compositeKey);
                
                // Create value: status:amount
                String valueStr = status + ":" + amount;
                outputValue.set(valueStr);
                
                context.write(outputKey, outputValue);
                
            } catch (Exception e) {
                // Log error and skip malformed records
                context.getCounter("TransactionMapper", "MALFORMED_RECORDS").increment(1);
            }
        }
    }

    public static class TransactionReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            int totalTransactions = 0;
            int completedTransactions = 0;
            int failedTransactions = 0;
            double totalRevenue = 0.0;
            double completedRevenue = 0.0;
            
            // Aggregate all values for this key
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String status = parts[0];
                    double amount = Double.parseDouble(parts[1]);
                    
                    totalTransactions++;
                    totalRevenue += amount;
                    
                    if ("completed".equals(status)) {
                        completedTransactions++;
                        completedRevenue += amount;
                    } else if ("failed".equals(status)) {
                        failedTransactions++;
                    }
                }
            }
            
            // Calculate metrics
            double successRate = totalTransactions > 0 ? 
                (double) completedTransactions / totalTransactions * 100.0 : 0.0;
            double avgTransactionAmount = totalTransactions > 0 ? 
                totalRevenue / totalTransactions : 0.0;
            
            // Create output: total_txns,completed_txns,failed_txns,total_revenue,completed_revenue,success_rate,avg_amount
            String resultStr = String.format("%d,%d,%d,%.2f,%.2f,%.2f,%.2f",
                totalTransactions, completedTransactions, failedTransactions,
                totalRevenue, completedRevenue, successRate, avgTransactionAmount);
            
            result.set(resultStr);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TransactionAggregationJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.name", "Transaction Aggregation Job");
        
        Job job = Job.getInstance(conf, "transaction aggregation");
        job.setJarByClass(TransactionAggregationJob.class);
        job.setMapperClass(TransactionMapper.class);
        job.setReducerClass(TransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
