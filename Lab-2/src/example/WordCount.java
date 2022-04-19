package example;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs= new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length!= 3) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job= Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IndexMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            Text fileName = new Text(fileSplit.getPath().getName());
            StringTokenizer itr= new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken() + " " + fileName);
                context.write(word, new Text("1"));
            }
        }
    }

    public static class IndexCombiner extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Text word = new Text();
            Text result = new Text();
            int sum = 0;
            for (Text value: values){
                sum += Integer.parseInt(value.toString());
            }
            String[] tem = Key.toString().split(" ");
            word.set(tem[0]);
            result.set(tem[1] + ":" + sum + ";");
            context.write(word, result);
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            int count_frequency = 0;
            int count_file = 0;
            StringBuilder s = new StringBuilder();
            for (Text value: values) {
                s.append(value.toString());
                String tem = value.toString().split(":")[1];
                tem = tem.substring(0, tem.length() - 1);
                count_frequency += Integer.parseInt(tem);
                count_file += 1;
            }
            s.insert(0, "\t");
            s.insert(0, new DecimalFormat("0.000").format(count_frequency * 1.0 / count_file));
            context.write(key, new Text(s.toString()));
        }
    }
}