
package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

import com.google.common.collect.Iterables;

public class WikiMostContributor {

	public static class WikiMostContributorMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static final Pattern USERNAME = Pattern.compile("<username>(.*)<\\/username>");
		private static final Text myKey = new Text("");
		private static final IntWritable One = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String articleXML = value.toString();

			List<String> usernames = getUsernames(articleXML);
			
			for (int i = 0; i < usernames.size(); i++) {
				myKey.set(usernames.get(i));
				context.write(myKey, One);
			}
		}

		private static List<String> getUsernames(CharSequence xml) {
			Matcher m = USERNAME.matcher(xml);
			List<String> matches = new ArrayList<String>();
			while(m.find()){
			    matches.add(m.group(1));
			}
			return matches;
		}

	}

	public static class WikiMostContributorReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

		long maxContributions = 0;
		Text mostContributor = new Text();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			long totalContributions = 0;
			for (IntWritable contributions : values) {
				totalContributions += contributions.get();
			}
			if (totalContributions > maxContributions) {
				mostContributor.set(key);
				maxContributions = totalContributions;
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(mostContributor, new LongWritable(maxContributions));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job.getInstance(conf, "WikiMostContributor");
		job.setJarByClass(WikiLongestArticle.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(WikiMostContributorMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(WikiMostContributorReducer.class);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}