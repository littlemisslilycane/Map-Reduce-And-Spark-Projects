import java.io.*;
import java.net.URI;
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

public class PageRank extends Configured implements Tool {
    public enum DANGLING_MASS {
        COUNTER_VAL
    }

    ;

    public static class GraphMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));
            double initial_rank = 1.0 / (k * k);
            while (itr.hasMoreTokens()) {
                String inputValue = itr.nextToken();
                if (inputValue != "") {
                    int v = Integer.parseInt(inputValue);
                    int j = v + k - 1;
                    while (v < j) {
                        int adj = v + 1;
                        //Emit the next node and rank
                        context.write(new Text(String.valueOf(v)), new Text("V:" + String.valueOf(adj) + "," + "R:" +
                                String.valueOf(initial_rank)));


                        v = adj;
                    }
                    //Point dangling node to zero node
                    context.write(new Text(String.valueOf(v)), new Text("V:" + String.valueOf("0") + "," + "R:" +
                            String.valueOf(initial_rank)));
                }
            }
        }
    }

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
        public static double counter = 0.0;

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] inputValues = itr.nextToken().split("-");
                if (inputValues.length == 2) {
                    String node = inputValues[0];
                    String[] values = inputValues[1].split(",");
                    String adjNode = values[0].split(":")[1];

                    double rank = Double.parseDouble(values[1].split(":")[1]);

                    if (adjNode.equals("0")) {
                        counter = counter + rank;
                    }
                    //Pass along the graph structure
                    context.write(new Text(node), new Text("V:" + adjNode));
                    //Compute contributions to send along outgoing links
                    context.write(new Text(adjNode), new Text("C:" + rank));
                }
            }
        }

        @Override
        public void cleanup(Context output) {


            long counter_temp = (long) (counter * (long) Math.pow(10, 10));
            output.getCounter(DANGLING_MASS.COUNTER_VAL).
                    increment(counter_temp);
            counter = 0.0;


        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        public static long mapperCounter;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            mapperCounter = currentJob.getCounters().findCounter(DANGLING_MASS.COUNTER_VAL).getValue();
            System.out.println("Mappercounter:" + mapperCounter);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text adjNode = new Text();
//            Double a = (Double) 0.125 * Math.pow(10, 10);
//            mapperCounter = a.longValue();
            Double contributions = 0.0;

            for (Text t : values) {
                String[] graphstructure = t.toString().split(",");
                for (String s : graphstructure) {
                    String symbol = s.split(":")[0];
                    String val = s.split(":")[1];
                    if (symbol.equals("V")) {
                        adjNode = new Text("V:" + val);
                    } else if (symbol.equals("C")) {
                        Double contribution = Double.parseDouble(val);
                        contributions += contribution;
                    }
                }
            }
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));
            Double randomWalk = 0.15 / (k * k);


            double probability_mass = mapperCounter / (Math.pow(10, 10));


            Double followPath = 0.85 * (contributions + (probability_mass / (k * k)));

            if (!key.toString().equals("0")) {

                context.write(key, new Text(String.valueOf(adjNode) + "," + "R:" +
                        String.valueOf(randomWalk + followPath)));
            }

        }
    }
/*
output format
1-V:2,R:0.25
2-V:0,R:0.25
3-V:4,R:0.25
4-V:0,R:0.25
 */

    private static void createInputFile(int k) throws IOException {
        File fout = new File("input/input-00/input.txt");
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (int i = 1; i < k * k; i = i + k ) {
            bw.write(String.valueOf(i));
            bw.newLine();
        }
        bw.close();
    }

    @Override
    public int run(final String[] args) throws Exception {

        final Configuration conf = getConf();
        conf.set("k", "2");
        Job job = Job.getInstance(conf, "Initialization");
        job.setJarByClass(PageRank.class);
        Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "-");
        job.setMapperClass(PageRank.GraphMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input/input-00"));
        FileOutputFormat.setOutputPath(job, new Path("input/input-0"));
        boolean jobSuccess = job.waitForCompletion(true);
        if (jobSuccess) {
            int i = 1;
            while (i <= 2) {
                Job job1 = Job.getInstance(conf, "PageRank");
                job1.setJarByClass(PageRank.class);
                Configuration jobConf1 = job1.getConfiguration();
                jobConf1.set("mapreduce.output.textoutputformat.separator", "-");
                job1.setMapperClass(PageRank.PageRankMapper.class);
                job1.setReducerClass(PageRank.PageRankReducer.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);
                String inputpath = "input/input-" + (i - 1);
                String outputpath = "input/input-" + i;
                FileInputFormat.addInputPath(job1, new Path(inputpath));
                FileOutputFormat.setOutputPath(job1, new Path(outputpath));
                job1.waitForCompletion(true);
                i++;
            }

        }
        return 1;
    }


    private static final Logger logger = LogManager.getLogger(PageRank.class);

    public static void main(final String[] args) {
        try {
             PageRank.createInputFile(2);
             ToolRunner.run(new PageRank(), args);
        } catch (final Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
