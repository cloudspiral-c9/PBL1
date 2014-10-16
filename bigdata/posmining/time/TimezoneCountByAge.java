package posmining.time;

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

public class TimezoneCountByAge {
	// MapReduceを実行するためのドライバ
		public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

			// MapperクラスとReducerクラスを指定
			Job job = new Job(new Configuration());
			job.setJarByClass(TimezoneCountByAge.class);   // ★このファイルのメインクラスの名前
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
			String outputpath = "out/timezoneCountByAge";    // ★MRの出力先
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
		 * <li>レシートIDをキーに、購入時間をバリューにしてemit
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
				if (csv[PosUtils.BUYER_AGE].equals("4") == false) {
					return;
				}

				String hour =csv[PosUtils.HOUR];

				String rid = csv[PosUtils.RECEIPT_ID];
				context.write(new CSKV(hour), new CSKV(rid));
			}
		}

		/**
		 *  Reducerクラスのreduce関数を定義<br/>
		 * <p>
		 * 以下が具体的なreduce法
		 * <ul>
		 * <li>レシートIDの数をカウント
		 * <li>購入時間をキーに、購入数をバリューにしてemit
		 * </ul>
		 * @author 紀之
		 *
		 */
		// Reducerクラスのreduce関数を定義
		public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
			protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

				int count=0;
				for (CSKV value : values) {
					count++;
				}
				//String hour = key.toString().split(",")[0];

				// emit
				context.write(key,new CSKV(count));
			}
		}
}
