
package bd.edu.nyu.cooccurence;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import bd.edu.nyu.utility.RelativeFreqPair;
import bd.edu.nyu.utility.StripMapWritable;
import bd.edu.nyu.utility.TextPair;

/**
   Implementation of the "stripes" algorithm for computing co-occurrence
 matrices from a large text collection. 

  Arguments 
  [input-path]
  [output-path]
  [window]
 [num-reducers]

 */
public class ComputeRelativeFrequencyStripes extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ComputeRelativeFrequencyStripes.class);

	private static class MyMapper extends
	Mapper<LongWritable, Text, Text, StripMapWritable> {
		private StripMapWritable map = new StripMapWritable();
		private Text textKey = new Text();

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException,
		InterruptedException {
			String text = line.toString();

			String[] terms = text.split("\\s+");

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				// skip empty tokens
				if (term.length() == 0 || terms.length == 1)
					continue;
				map.clear();
				for (int j = 0; j < terms.length; j++) {
					if (j == i)
						continue;

					// skip empty tokens
					if (terms[j].length() == 0)
						continue;

					Text mapTextKey = new Text();
					mapTextKey.set(terms[j]);
					if (map.containsKey(mapTextKey)) {
						map.increment(mapTextKey);
					} else {
						map.put(mapTextKey, new IntWritable(1));
					}
				}
				textKey.set(term);
				context.write(textKey, map);
			}
		}
	}
	
	private static class MyCombiner extends
	Reducer<Text, StripMapWritable, Text, StripMapWritable> {

		@Override
		public void reduce(Text key, Iterable<StripMapWritable> values, Context context)
				throws IOException,
				InterruptedException {
			Iterator<StripMapWritable> iter = values.iterator();
			StripMapWritable map = new StripMapWritable();
			while (iter.hasNext()) {
				map.plus(iter.next());
			}
			context.write(key, map);
		}
	}

	private static class MyReducer extends
	Reducer<Text, StripMapWritable, TextPair, DoubleWritable> {
		private final static DoubleWritable SumValue = new DoubleWritable();
		private TreeSet<RelativeFreqPair> priorityQueue = new TreeSet<>();

		@Override
		public void reduce(Text key, Iterable<StripMapWritable> values, Context context)
				throws IOException,
				InterruptedException {
			double totalCount = 0.0; 
			Iterator<StripMapWritable> iter = values.iterator();

			StripMapWritable map = new StripMapWritable();

			while (iter.hasNext()) {
				map.plus(iter.next());
			}
			iter = values.iterator();
			for (Map.Entry<Writable, Writable> e : map.entrySet()){
				IntWritable val = new IntWritable(0);
				val = (IntWritable)(e.getValue());
				totalCount += val.get();
			}
			for (Map.Entry<Writable, Writable> e : map.entrySet()){
				RelativeFreqPair rfp;
				Text mapKey = (Text)e.getKey();
				TextPair pair = new TextPair();
				pair.set(key.toString(), mapKey.toString());
				IntWritable val = new IntWritable(0);
				val = (IntWritable)(e.getValue());

				rfp = new RelativeFreqPair(val.get()/totalCount, pair);
				priorityQueue.add(rfp);

				if (priorityQueue.size() > 100) {
					priorityQueue.pollFirst();
				}
			}

		}

		protected void cleanup(Context context)
				throws IOException,
				InterruptedException {
			while (!priorityQueue.isEmpty()) {
				RelativeFreqPair pair = priorityQueue.pollLast();
				SumValue.set(pair.relativeFrequency);
				context.write(pair.key, SumValue);
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public ComputeRelativeFrequencyStripes() {
	}

	private static int printUsage() {
		System.out
		.println("usage: [input-path] [output-path] [window] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);

		sLogger.info("Tool: ComputeRelativeFrequencyStripes");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);

		Job job = new Job(getConf(), "ComputeRelativeFrequencyStripes");

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		URI uri = URI.create(outputPath);
		FileSystem.get(uri,getConf()).delete(outputDir, true);

		job.setJarByClass(ComputeRelativeFrequencyStripes.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StripMapWritable.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);

		job.setNumReduceTasks(reduceTasks);
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ComputeRelativeFrequencyStripes(), args);
		System.exit(res);
	}
}
