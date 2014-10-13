package posmining.pbl1.sake;

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

public class CountItemNumBoughtWithSake2 {

	private static final String JOBNAME = "2014003";
	private static final String INPUTPATH = "out/CountItemNumBoughtWithSake";
	private static final String OUTPUTPATH = "out/CountItemNumBoughtWithSake2";

	private static final Class<CountItemNumBoughtWithSake2> JARCLASS = CountItemNumBoughtWithSake2.class;
	private static final Class<CountItemNumBoughtWithSake2Mapper> MAPPERCLASS = CountItemNumBoughtWithSake2Mapper.class;
	private static final Class<CountItemNumBoughtWithSake2Reducer> REDUCERCLASS = CountItemNumBoughtWithSake2Reducer.class;

	private static final String[] sakeList = { "ビール", "酒", "チューハイ", "ワイン",
			"ウイスキー", "カクテル" };

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = new Job(new Configuration());
		job.setJarByClass(JARCLASS);

		// 入出力形式の指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// アウトプットのキーバリューの型をそれぞれ設定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// インプットファイルのパスを指定
		if (args.length > 0) {
			FileInputFormat.setInputPaths(job, new Path(args[0]));
		} else {
			FileInputFormat.setInputPaths(job, new Path(INPUTPATH));
		}
		FileOutputFormat.setOutputPath(job, new Path(OUTPUTPATH));
		// いったんアウトプットパスを削除して，ロック状態を解放しておく
		PosUtils.deleteOutputDir(OUTPUTPATH);

		// タスク数の設定
		job.setNumReduceTasks(8);

		// MapperとReducerの設定
		job.setJobName(JOBNAME);
		job.setMapperClass(MAPPERCLASS);
		job.setReducerClass(REDUCERCLASS);

		job.waitForCompletion(true);

	}

	public static class CountItemNumBoughtWithSake2Mapper extends
			Mapper<LongWritable, Text, CSKV, CSKV> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String csv[] = value.toString().split("\t")[1].split(",");

			String itemName = csv[0];
			String itemCount = csv[1];
			String itemCategoryName = csv[2];

			String ky = itemName + "," + itemCategoryName;

			context.write(new CSKV(ky), new CSKV(itemCount));
		}

	}

	public static class CountItemNumBoughtWithSake2Reducer extends
			Reducer<CSKV, CSKV, CSKV, CSKV> {

		protected void reduce(CSKV key, Iterable<CSKV> values, Context context)
				throws IOException, InterruptedException {

			String itemCategoryName = key.toString().split(",")[1];

			for (String sake : sakeList) {
				if (itemCategoryName.matches(".*" + sake + ".*")) {
					return;
				}
			}

			long totalCount = 0;

			for (CSKV value : values) {
				totalCount += value.toLong();
			}

			context.write(new CSKV(totalCount),key);
		}
	}

}