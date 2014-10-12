package posmining.enshu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

public class OnigiriSalesByDate {


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Job job = new Job(new Configuration());
		job.setJarByClass(OnigiriSalesByDate.class);
		job.setMapperClass(OnigiriMapper.class);
		job.setReducerClass(OnigiriReducer.class);
		job.setJobName("2014012");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		String inputpath = "posdata";
		String outputpath = "out/onigiriSalesByDate";

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		PosUtils.deleteOutputDir(outputpath);

		job.setNumReduceTasks(8);

		job.waitForCompletion(true);

	}

	public static class OnigiriMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String csv[] = value.toString().split(",");

			//おにぎり以外は無視
			if (!csv[PosUtils.ITEM_CATEGORY_NAME].equals("おにぎり・おむすび")) {
				return;
			}

			int count;
			int price;
			String time;
			try {
				count = Integer.parseInt(csv[PosUtils.ITEM_COUNT]);
				price = Integer.parseInt(csv[PosUtils.ITEM_PRICE]);
				time = csv[PosUtils.YEAR] + csv[PosUtils.MONTH] + csv[PosUtils.DATE];
			} catch (Exception e) {
				return;
			}

			int totalPrice = count * price;

			context.write(new CSKV(time), new CSKV(totalPrice));
		}

	}

	public static class OnigiriReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {

		protected void reduce(CSKV key, Iterable<CSKV> prices, Context context) throws IOException, InterruptedException {

			int totalPrice = 0;
			for (CSKV price : prices) {
				totalPrice += price.toInt();
			}

			context.write(key, new CSKV(totalPrice));
		}
	}

}
