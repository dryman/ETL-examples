package org.idryman.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class OneToMany extends Configured implements Tool{
  private static final Joiner joiner = Joiner.on("\t");

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new OneToMany(), args));
  }
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = new Job(conf, "Join example");
    job.setJarByClass(this.getClass());
    
    
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DimensionMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FactMapper.class);

    job.setMapOutputKeyClass(SecondarySortContainer.class);
    job.setMapOutputValueClass(SecondarySortContainer.class);
    job.setReducerClass(JoinReducer.class);
    job.setNumReduceTasks(10);
    
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.submit();
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class DimensionMapper extends Mapper<LongWritable, Text, SecondarySortContainer<Text>, SecondarySortContainer<Text>> {
    private static final Joiner joiner = Joiner.on("\t");
    private SecondarySortContainer<Text> key_out = new SecondarySortContainer<Text>(new Text(), 0);
    private SecondarySortContainer<Text> val_out = new SecondarySortContainer<Text>(new Text(), 0);
    @Override
    protected void map(LongWritable key_in, Text val_in, Context context) throws IOException, InterruptedException {
      String [] fields = val_in.toString().split("\\t");
      key_out.getValue().set(joiner.join(fields[0], fields[1]));
      val_out.getValue().set(joiner.join(fields[2], fields[3]));
      context.write(key_out, val_out);
    }
  }
  
  public static class FactMapper extends Mapper<LongWritable, Text, SecondarySortContainer<Text>, SecondarySortContainer<Text>> {
    private SecondarySortContainer<Text> key_out = new SecondarySortContainer<Text>(new Text(), 1);
    private SecondarySortContainer<Text> val_out = new SecondarySortContainer<Text>(new Text(), 1);
    @Override
    protected void map(LongWritable key_in, Text val_in, Context context) throws IOException, InterruptedException {
      String [] fields = val_in.toString().split("\\t");
      key_out.getValue().set(joiner.join(fields[0], fields[1]));
      val_out.getValue().set(joiner.join(fields[2], fields[3], fields[4], fields[5])); // fact table is usually larger
      context.write(key_out, val_out);
    }
  }
  
  public static class JoinReducer extends Reducer<SecondarySortContainer<Text>, SecondarySortContainer<Text>, Text, NullWritable> {
    private Text     key_out = new Text();
    @Override
    protected void reduce(SecondarySortContainer<Text> key_in, Iterable<SecondarySortContainer<Text>> vals, Context context) throws IOException, InterruptedException{
      Iterator<SecondarySortContainer<Text>> val_it = vals.iterator();
      SecondarySortContainer<Text> first = val_it.next();
      Preconditions.checkState(first.getOrder() == 0, "First entry should be a dimension, else we're having a fact entry missing matching dimension");
      String dim = first.getValue().toString();
      
      
      while (val_it.hasNext()) {
        SecondarySortContainer<Text> other = val_it.next();
        Preconditions.checkState(first.getOrder() == 1, "Ohter entries should be fact, else we're having duplicated dimesions");
        String fact = other.getValue().toString();
        key_out.set(fact+"\t"+dim);
        context.write(key_out, NullWritable.get());
      }
      
      /*
       * Many-to-many join, left join are all similar to this template
       * the only difference is how you handle order numbers:
       * one-to-many: 0, 1, 1, 1
       * left join can be either: 0, 1, 1, 1, or 1, 1, 1, 1
       * many-to-many, or even cross-join can have multiple 0s and 1s: 0, 0, 0, 1, 1, 1, all 0s, or all 1s
       */
    }
  }
}
