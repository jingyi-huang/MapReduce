import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;


public class Ngramstatistics {
    public static class Map1 extends Mapper<Object, Text, Text, IntWritable> {
        private static IntWritable one;
        private Text gramWord = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String newString = value.toString().replace(" ", "");
            StringBuilder sb = new StringBuilder();
            // Get n as a parameter
            int left = 0, right = 0, ngram = Integer.parseInt(context.getConfiguration().get("grams"));
            while (right < newString.length()) {
                sb.append(newString.charAt(right++));

                if (right - left == ngram) {
                    gramWord.clear();
                    gramWord.set(sb.toString());
                    context.write(gramWord, one);
                    left++;
                    sb.delete(0, 1);
                }
            }


        }

        @Override
        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            one = new IntWritable(1);
        }

    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public HashMap<String, Integer> map;

        public void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            this.map = new HashMap<>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            map.put(key.toString(), sum);
        }

        public void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o1.getValue() > o2.getValue() ? -1 : o1.getValue() == o2.getValue() ? 0 : 1;
                }
            });
            Text text = new Text();
            IntWritable intWritable = new IntWritable();
            for (Map.Entry<String, Integer> entry : list) {
                text.set(entry.getKey());
                intWritable.set(entry.getValue());
                context.write(text, intWritable);
                text.clear();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // FileUtil.fullyDelete(new File(args[1]));
        Configuration conf = new Configuration();
        conf.set("grams", args[2]);
        Job job = Job.getInstance(conf, "ngram");
        // job.setNumReduceTasks(0);
        job.setJarByClass(Ngramstatistics.class);
        job.setMapperClass(Map1.class);
        job.setCombinerClass(Reduce1.class);
        job.setReducerClass(Reduce1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextOutputFormat.setOutputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        System.out.println("Done.");

    }


}