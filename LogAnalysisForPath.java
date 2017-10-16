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

/**
 * Created by kaiyanglyu on 2/25/17.
 */
public class LogAnalysisForPath {

    public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
        private static IntWritable one;
        private Text text = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String file = value.toString();
            StringTokenizer st = new StringTokenizer(file, "\n");

            while (st.hasMoreTokens()) {

                String path = st.nextToken().toString().split(" ")[6];
                path = path.contains(".") ? path : (path.endsWith("/")) ? path : path + "/";
                text.set(path);
                context.write(text, one);
                text.clear();
            }


        }

        @Override
        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            one = new IntWritable(1);
        }

    }

    public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public HashMap<String, Integer> map;

        public void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            this.map = new HashMap<>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (map.get(key.toString()) != null)
                map.put(key.toString(), map.get(key.toString()) + sum);
            else map.put(key.toString(), sum);
        }

        public void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            Text text = new Text();
            IntWritable intWritable = new IntWritable();
            String target = context.getConfiguration().get("parameter");
            if (target.equals("null")) {
                List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
                Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                    @Override
                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                        return o1.getValue() > o2.getValue() ? -1 : o1.getValue() == o2.getValue() ? 0 : 1;
                    }
                });
                for (Map.Entry<String, Integer> entry : list) {
                    text.set(entry.getKey());
                    intWritable.set(entry.getValue());
                    context.write(text, intWritable);
                    text.clear();
                }
            } else {
                text.set(target);
                intWritable.set(map.get(target));
                context.write(text, intWritable);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        // FileUtil.fullyDelete(new File(args[1]));
        Configuration conf = new Configuration();
        // check whether is null at reduce.cleanup()
        conf.set("parameter", args[2]);
        Job job = Job.getInstance(conf, "analysis");

        // job.setNumReduceTasks(0);
        job.setJarByClass(LogAnalysisForPath.class);
        job.setMapperClass(Mapper1.class);
        job.setCombinerClass(Reducer1.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.waitForCompletion(true);
        System.out.println("Done.");

    }
}
