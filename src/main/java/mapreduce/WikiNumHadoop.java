package mapreduce;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

import com.google.common.collect.Iterables;

public class WikiNumHadoop {

	public static class WikiNumHadoopMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static final String START_DOC = "<text xml:space=\"preserve\">";
		private static final String END_DOC = "</text>";
		private static final Pattern TITLE = Pattern.compile("<title>(.*)<\\/title>");
		private static final Text myKey = new Text("key");
		private static final IntWritable One = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String articleXML = value.toString();

			String title = getTitle(articleXML).toLowerCase();
			String document = getDocument(articleXML).toLowerCase();
			String toFind = new String("hadoop");
			
			if (title.indexOf(toFind) != -1 || document.indexOf(toFind) != -1) {
				context.write(myKey, One);
			}
		}

		private static String getDocument(String xml) {
			int start = xml.indexOf(START_DOC) + START_DOC.length();
			int end = xml.indexOf(END_DOC, start);
			return start < end ? xml.substring(start, end) : "";
		}

		private static String getTitle(CharSequence xml) {
			Matcher m = TITLE.matcher(xml);
			return m.find() ? m.group(1) : "";
		}

	}

	public static class WikiNumHadoopReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
		
		LongWritable number = new LongWritable(0);

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			number.set(number.get() + Iterables.size(values));
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text("Number of occurences of `hadoop` : "), number);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job.getInstance(conf, "WikiNumHadoop");
		job.setJarByClass(WikiLongestArticle.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(WikiNumHadoopMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(WikiNumHadoopReducer.class);
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}