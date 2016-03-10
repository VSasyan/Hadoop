
package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
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

public class WikiMostCommunWords {

	public static class WikiMostCommunWordsMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static final String START_DOC = "<text xml:space=\"preserve\">";
		private static final String END_DOC = "</text>";
		private static final Pattern TITLE = Pattern.compile("<title>(.*)<\\/title>");
		private static final String WORD = "[a-zA-Z]+(-?[a-zA-Z]+)";

		private static final Text myKey = new Text("");
		private static final IntWritable One = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String articleXML = value.toString();

			String title = getTitle(articleXML);
			String document = getDocument(articleXML);
			
			String all = title + ' ' + document;
			
			StringTokenizer itr = new StringTokenizer (all.toString(), " \t\n\r\f,.:;?![]'");
			
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken();
				if (word.matches(WORD)) {
					myKey.set(word);
					context.write(myKey, One);
				}
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

	public static class WikiMostCommunWordsReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
		
		TreeMap<Long, ArrayList<String>> treeMap = new TreeMap<Long, ArrayList<String>>();
		
		public void reduce(ArrayList<String> key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			long totalOccurences = 0;
			for (IntWritable occurences : values) {
				totalOccurences += occurences.get();
			}
			ArrayList<String> list = treeMap.get(totalOccurences);
			if (list == null) {
				list = new ArrayList<String>();
				treeMap.put(totalOccurences, key);
			}
			
			// add to TreeMap
			treeMap.put(totalOccurences, key);
			
			if (treeMap.size() > 100) {
				treeMap.remove(treeMap.firstEntry().getKey());
			}
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
		
			for (Entry<Long, ArrayList<String>> entry : treeMap.descendingMap().entrySet()) {
				context.write(new Text(entry.getValue().toString()), new LongWritable(entry.getKey()));
			}
		}
	}
	
	public static class WikiMostCommunWordsMapper2 extends Mapper<Object, Text, Text, IntWritable> {
		
	}
	
	public static class WikiMostCommunWordsReducer2 extends Reducer<Text, IntWritable, Text, LongWritable> {
		
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job.getInstance(conf, "WikiMostCommunWords");
		job.setJarByClass(WikiLongestArticle.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(WikiMostCommunWordsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(WikiMostCommunWordsReducer.class);
		job.setNumReduceTasks(20);

		/*Job job2 = Job.getInstance(conf, "2_WikiMostCommunWords_2");
		job2.setJarByClass(WikiLongestArticle.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		job2.setInputFormatClass(XmlInputFormat.class);
		job2.setMapperClass(WikiMostCommunWordsMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		job2.setReducerClass(WikiMostCommunWordsReducer2.class);
		job2.setNumReduceTasks(4);*/

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}