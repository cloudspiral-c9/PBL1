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

public class kindCountByAge1 {
	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(kindCountByAge1.class);   // ★このファイルのメインクラスの名前
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
		String inputpath = "posdata";
		String outputpath = "out/kindCountByAge1";    // ★MRの出力先
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
	 * <p>
	 * 以下が具体的なmapping
	 * <ul>
	 * <li>実年でないレシートは除外
	 * <li>レシートID,商品カテゴリ名をキーに、個数をバリューにしてemit
	 * </ul>
	 * @author 紀之
	 *
	 */
	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// 実年でないレシートは無視
			if (!"4".equals(csv[PosUtils.BUYER_AGE])) {
				return;
			}

			String count =csv[PosUtils.ITEM_COUNT];

			String rid_category = csv[PosUtils.RECEIPT_ID] + ","+csv[PosUtils.ITEM_CATEGORY_NAME];
			context.write(new CSKV(rid_category), new CSKV(count));
		}
	}

	/**
	 *  Reducerクラスのreduce関数を定義<br/>
	 * <p>
	 * 以下が具体的なreduce法
	 * <ul>
	 * <li>個数の数をカウント
	 * <li>レシートID,商品カテゴリーをキーに、カテゴリ数をバリューにしてemit
	 * </ul>
	 * @author 紀之
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
