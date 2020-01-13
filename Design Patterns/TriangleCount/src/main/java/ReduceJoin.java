import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import testjar.ClassWordCount;

public class ReduceJoin extends Configured implements Tool {
    public enum TRIANGLE_COUNTER {
        COUNTER_VAL
    }

    ;

    public static class FilterMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();
            int max = Integer.parseInt(conf.get("MAX"));

            while (itr.hasMoreTokens()) {
                String[] inputValues = itr.nextToken().split(",");
                if (inputValues.length == 2) {
                    if (Integer.parseInt(inputValues[0]) <= max &&
                            Integer.parseInt(inputValues[1]) <= max) {
                        context.write(new Text(inputValues[0]), new Text(inputValues[1]));

                    }
                }

            }
        }
    }

    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {

        private ArrayList<Integer> listA = new ArrayList<>();
        private ArrayList<Integer> listB = new ArrayList<>();


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            for (Text t : values) {
                context.write(null, new Text(key + "," + t));
            }
        }

    }

    public static class Path2Mapper extends Mapper<Object, Text, Text, MyWritable> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String[] inputValues = itr.nextToken().split(",");
                if (inputValues.length == 2) {
                    MyWritable valueObject = new MyWritable(new Integer(inputValues[0]), new Integer(inputValues[1]), "XtoY");
                    context.write(new Text(inputValues[1]), valueObject); // x to y
                    valueObject = new MyWritable(new Integer(inputValues[0]), new Integer(inputValues[1]), "YtoZ");
                    context.write(new Text(inputValues[0]), valueObject); // y to z
                }
            }
        }
    }

    public static class TripleCountMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String[] inputValues = itr.nextToken().split(",");
                if (inputValues.length == 3) {

                    context.write(new Text(new String(inputValues[0] + "," + inputValues[1] + "," + inputValues[2])), new Text(""));
                    context.write(new Text(inputValues[1] + "," + inputValues[2] + "," + inputValues[0]), new Text(""));
                    context.write(new Text(inputValues[2] + "," + inputValues[0] + "," + inputValues[1]), new Text(""));
                }
            }
        }
    }

    public static class TripleCountReducer extends Reducer<Text, Text, Text, Text> {
        private static int counter;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long count = 0;
            for (Text t : values) {
                ++count;

            }

            if (count == 3) {
                ++counter;
                context.write(new Text(""), key);
            }
        }

        @Override
        public void cleanup(Context output) {

            output.getCounter(TRIANGLE_COUNTER.COUNTER_VAL).increment(counter);
            counter = 0;


        }

    }

    public static class Path2Reducer extends Reducer<Text, MyWritable, Text, Text> {

        private ArrayList<Integer> listA = new ArrayList<>();
        private ArrayList<Integer> listB = new ArrayList<>();


        @Override
        public void reduce(Text key, Iterable<MyWritable> values, Context context)
                throws IOException, InterruptedException {

            listA.clear();
            listB.clear();

            for (MyWritable t : values) {
                if (t.getType().equals("XtoY")) {
                    listA.add(t.getX());
                } else if (t.getType().equals("YtoZ")) {
                    listB.add(t.getY());
                }
            }

            for (int s : listA) {
                for (int t : listB) {
                    if (s != t) {
                        context.write(null, new Text(s + "," + key + "," + t));
                    }

                }
            }

        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("MAX", "1000");
        final Job job = Job.getInstance(conf, "MaxFilter");
        job.setJarByClass(ClassWordCount.Reduce.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean jobSuccess = job.waitForCompletion(true);
        if (jobSuccess) {
            final Job job1 = Job.getInstance(conf, "ReduceJoin");
            job1.setJarByClass(ReduceJoin.class);
            final Configuration jobConf1 = job1.getConfiguration();
            jobConf1.set("mapreduce.output.textoutputformat.separator", ",");


            job1.setMapperClass(Path2Mapper.class);
            job1.setReducerClass(Path2Reducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(MyWritable.class);
            FileInputFormat.addInputPath(job1, new Path(args[1]));
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            boolean job1Success = job1.waitForCompletion(true);

            if (job1Success) {
                final Job job2 = Job.getInstance(conf, "TriangleCount");
                job2.setJarByClass(ReduceJoin.class);
                jobConf.set("mapreduce.output.textoutputformat.separator", ",");

                job2.setMapperClass(TripleCountMapper.class);
                job2.setReducerClass(TripleCountReducer.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                job2.setNumReduceTasks(1);
                FileInputFormat.addInputPath(job2, new Path(args[2]));
                FileOutputFormat.setOutputPath(job2, new Path(args[3]));
                boolean job2Success = job2.waitForCompletion(true);
                if (job2Success) {
                    Counters cn = job2.getCounters();
                    System.out.println("Displaying the triangle count " + cn.findCounter(TRIANGLE_COUNTER.COUNTER_VAL).getValue() / 3);
                    return 1;
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        } else {
            return 0;
        }
    }

    private static final Logger logger = LogManager.getLogger(MaxFilter.class);

    public static void main(final String[] args) {


        if (args.length != 4) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ReduceJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
