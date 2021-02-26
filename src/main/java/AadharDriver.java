/**
 * Developer: Vikramraj Sahu
 *
 * Problem Statements
 * 1. Count the Male Residents and Female Resident in each state who have enrolled in Aadhaar
 * 2. Count the rejected residents in each state
 * 3. Count the rejected residents in each state and then calculate the average of the rejected residents in India
 *
 * Schema:
 * Registrar,Enrolment Agency,State,District,Sub District,Pin Code,Gender,Age,Aadhaar generated,Enrolment Rejected,Residents providing email,Residents providing mobile number
 *
 * Sample Data:
 *  Chief Registrar Births & Deaths -cum-Director Health Services ,District Registrar Births & Deaths cum Chief Medical Officer  Mandi,Himachal Pradesh,Mandi,Bali Chowki,174402,F,0,1,1,1,1
 *  Chief Registrar Births & Deaths -cum-Director Health Services ,District Registrar Births & Deaths cum Chief Medical Officer  Mandi,Himachal Pradesh,Mandi,Bali Chowki,175001,M,1,1,0,0,0
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class AadharDriver extends Configured implements Tool {

    /** START
     * Case 1: Count the Male Residents and Female Resident in each state who have enrolled in Aadhaar
     */
    public static class GenderStateWiseCountMapper extends Mapper<LongWritable, Text, Text,Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");
            if(columns.length != 12) {
                return;
            }

            String state = columns[2];
            String gender = columns[6];

            context.write(new Text(state), new Text(gender));
        }
    }
    public static class GenderStateWiseCounterReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text keyState, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            String gender;
            long maleCount = 0;
            long femaleCount = 0;
            for(Text value:values) {
                gender = value.toString();
                if(gender.contentEquals("M")) {
                    maleCount++;
                } else if(gender.contentEquals("F")) {
                    femaleCount++;
                }
            }

            context.write(keyState, new Text("Total Male Enrolled: " + maleCount + "\\tTotal Female Enrolled: " + femaleCount ));
        }
    }
    /** Case 1 End */

    /** START
     * Case 2: Count the rejected residents in each state
     */
    public static class RejectedStateWiseCountMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] columns = line.split(",");
            if(columns.length != 12) {
                return;
            }

            String state = columns[2];
            byte rejected = Byte.parseByte(columns[9]);
            if(rejected == 1) {
                context.write(new Text(state), new IntWritable(1));
            }
        }
    }
    public static class RejectedStateWiseCounterReducer extends Reducer<Text,IntWritable,Text,LongWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {

            long totalRejected = 0;
            for(IntWritable value : values) {
                totalRejected += value.get();
            }

            context.write(key, new LongWritable(totalRejected));

        }
    }
    /** Case 2 End */

    /** START
     * Case 3: Count the rejected residents in each state and then calculate the average of the rejected residents in India
     */
    public static class AvgRejectedStateWiseCountMapper extends Mapper<LongWritable, Text, Text,LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
            String state = key.toString();
            String line = value.toString();
            String[] columns = line.split("\\t");

            if(columns.length != 2) {
                return;
            }

            long stateWiseRejectionCount = Long.parseLong(columns[1]);
            context.write(new Text("1"), new LongWritable(stateWiseRejectionCount));
        }
    }
    public static class AvgRejectedStateWiseCounterReducer extends Reducer<Text,LongWritable,Text,DoubleWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,InterruptedException {
                long totalStates = 0;
                long totalRejections = 0;
                for(LongWritable value: values) {
                    totalStates++;
                    totalRejections += Long.parseLong(value.toString());
                }

                double average = totalRejections/(double) totalStates;
                context.write(new Text("Average in India"), new DoubleWritable(average));

        }
    }
    /** Case 3 End */

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new AadharDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job;
        job = Job.getInstance(getConf(),"Case 1: Analyzing Aadhar Data using Java MapReduce API");

        job.setJarByClass(AadharDriver.class);

        /** Case 1 */
        job.setMapperClass(GenderStateWiseCountMapper.class);
        job.setReducerClass(GenderStateWiseCounterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("Aadhar/data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("Aadhar/gender-state-wise-count"));

        if(!job.waitForCompletion(true)) {
            return 0;
        }

        /** Case 2 */
        job = Job.getInstance(getConf(),"Case 2: Analyzing Aadhar Data using Java MapReduce API");

        job.setJarByClass(AadharDriver.class);
        job.setMapperClass(RejectedStateWiseCountMapper.class);
        job.setReducerClass(RejectedStateWiseCounterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("Aadhar/data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("Aadhar/rejected-count"));

        if(!job.waitForCompletion(true)){
            return 0;
        }

        /** Case 3 */
        job = Job.getInstance(getConf(),"Case 3: Analyzing Aadhar Data using Java MapReduce API");

        job.setJarByClass(AadharDriver.class);
        job.setMapperClass(AvgRejectedStateWiseCountMapper.class);
        job.setReducerClass(AvgRejectedStateWiseCounterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);

        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("Aadhar/rejected-count"));
        FileOutputFormat.setOutputPath(job, new Path("Aadhar/average-count"));

        return job.waitForCompletion(true)?1:0;
    }
}
