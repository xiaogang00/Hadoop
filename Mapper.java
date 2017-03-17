import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import jdk.nashorn.internal.runtime.arrays.IteratorAction;

public class MaxTemperatureMapper
  extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWtitable>{
    private static final int MISSING = 9999;
    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException{
        String line = value.toString();
        String year = line.substring(15,19);
        int airTemperature;
        if(line.charAt(87) == '+'){
          airTemperature = Integer.parseInt(line.substring(88,92));
        }
        else{
          airTemperature = Integer.parseInt(line.substring(87,92));
        }
        String quality = line.substring(92,93);
        if(airTemperature != MISSING && quality.matches("[01459]")){
          context.write(new Text(year), new IntWritable(airTemperature));
        }
      }
  }


//Reducer 类
public class MaxTemperatureReducer
   extends Reducer<Text, IntWritable, Text, IntWritable>{
     @Override
     public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{
          int maxValue = Integer.MIN_VALUE;
          for (IntWritable value :values){
            maxValue = Math.max(maxValue, value.get());
          }
          context.write(key, new IntWritable(maxValue));
     }
   }
//应用这个程序在气象数据集中找出最高气温
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.input.FileOutputFormat;
import org.apache.hadoop.mapredduce.input.FileOutputFormat;

public class MaxTemperature{
  public static void main(String[] args) throws Exception{
    if(args.length != 2){
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    Job job = new Job();
    job.setJarByClass(MaxTemperature.class);
    job.setJobName("Max Temperature");
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}







