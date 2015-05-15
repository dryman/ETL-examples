package org.idryman.aggregation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;

public class BasicAggregation extends Configured implements Tool{
  public BasicAggregation () {}
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new BasicAggregation(), args));
  }
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = new Job(conf, "Aggregation example");
    job.setJarByClass(this.getClass());
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    
    job.setMapperClass(AggMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AggValue.class);
    job.setReducerClass(AggCombiner.class);
    job.setCombinerClass(AggCombiner.class);
    job.setNumReduceTasks(1);
    
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.submit();
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class AggMapper extends Mapper<LongWritable, Text, Text, AggValue> {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Joiner joiner = Joiner.on("\t"); // joiner is also a common utility to use
    private long     start, end;
    private Text     key_out = new Text();
    private AggValue val_out = new AggValue();
    @Override 
    protected void setup(Context context) throws IOException, InterruptedException{
      try {
        Configuration conf = context.getConfiguration();
        start = sdf.parse(conf.get("basic-agg.start")).getTime();
        end   = sdf.parse(conf.get("basic-agg.end")).getTime();
      } catch (ParseException e) {
        throw new InterruptedException("Failure on setup");
      }
    }
    @Override
    protected void map(LongWritable key_in, Text val_in, Context context) throws IOException, InterruptedException {
      String [] fields = val_in.toString().split("\\t");
      /*
       * timestamp, country, state, city, temperature
       */
      long   time = Long.parseLong(fields[0]);
      String country = fields[1], state = fields[2], city = fields[3];
      long   temperature = (long) (Float.parseFloat(fields[4]) * 1000_000);
      // Use long over float/double is preferred to get better precision
      if (time < start || time >= end) return; // close-open interval [start, end) is common practice
      
      key_out.set(joiner.join(country,state,city));
      val_out.count = 1;
      val_out.sum   = temperature;
      
      context.write(key_out, val_out);
    }
  }
  
  public static class AggCombiner extends Reducer<Text, AggValue, Text, AggValue> {
    private AggValue out_value = new AggValue();
    @Override
    protected void reduce(Text key, Iterable<AggValue> vals, Context context) throws IOException, InterruptedException {
      out_value.reset();
      for (AggValue val : vals) {
        out_value.accumulate(val);
      }
      context.write(key, out_value);
    }
  }
  
  public static class AggValue implements Writable, Accumulator<AggValue>{
    public long sum;
    public long count;
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(sum);
      out.writeLong(count);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      sum   = in.readLong();
      count = in.readLong();
    }
    @Override
    public void accumulate(AggValue other) {
      sum   += other.sum;
      count += other.sum;
    }
    /**
     * {@link TextOutputFormat} use toString() for all non-Text classes
     */
    @Override
    public String toString() {
      return sum + "\t" + count;
    }
    @Override
    public void reset() {
      sum = 0;
      count = 0;
    }
  }
}
