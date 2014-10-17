package posmining.kind;

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

public class kindCountByAge2 {
	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(kindCountByAge2.class);   // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014034");                        // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "out/kindCountByAge1";
		String outputpath = "out/kindCountByAge2";    // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}

	/**
	 * Mapperクラスのmap関数を定義<br/>
	 * 年度別の商品カテゴリの売り上げをやろうとしたけど、種類が多すぎるのでやめました
	 * <p>
	 * 以下が具体的なmapping
	 * <ul>
	 * <li>","で分割してカテゴリ名を取得
	 * <li>カテゴリ名をキーに、販売個数をバリューにしてemit
	 * </ul>
	 * @author 紀之
	 *
	 */
	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// タブで分割
			String csv[] = value.toString().split("\t");

			String rid = csv[0];
			String category = rid.split(",")[1];

			context.write(new CSKV(category), new CSKV(1));
		}
	}

	/**
	 *  Reducerクラスのreduce関数を定義<br/>
	 * <p>
	 * 以下が具体的なreduce法
	 * <ul>
	 * <li>カテゴリ別の販売個数を足す
	 * <li>キーをそのままに、足し上げた販売個数をバリューにしてemit
	 * </ul>
	 * @author 紀之
	 *
	 */
	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			long count=0;
			for (CSKV value : values) {
				count += value.toLong();
			}
			// emit
			context.write(key,new CSKV(count));
		}
	}


}