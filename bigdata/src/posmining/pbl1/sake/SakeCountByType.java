package pbl1;


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

/**
 * 酒-種類別の個数を出力する:add comment
 * @author thuy
 *
 */
public class SakeCountByType {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(SakeCountByType.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014019");                        // ★自分の学籍番号

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
		String outputpath = "out/sakeCountByType";     // ★MRの出力先
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


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");


			int code = Integer.parseInt(csv[PosUtils.ITEM_CATEGORY_CODE]);
			// valueとなる販売個数を取得
			String count = csv[PosUtils.ITEM_COUNT];

			String type = "etc";
			// ★ タイプ：日本酒
			if ( (code>=211031 && code <= 211039 ) || (code == 211041 ) || (code == 211042 ) ||
					(code == 211011 ) || (code == 211021 ) || (code == 211999 ) ) {
				type = "日本酒";
			}

			// ★ タイプ：ビール
			if (  (code == 212001 ) || (code == 212002 ) ||
					(code == 212011 ) || (code == 212012 ) || (code == 212999 ) ) {
				type = "ビール";
			}

			// ★ タイプ：発泡酒
			if (  (code>=221001 && code <= 221003 )  ||
					(code == 221999 )  ) {
				type = "発泡酒";
			}

			// ★ タイプ：ワイン
			if (  (code>=215001 && code <= 215003 )  || (code>=215011 && code <= 215014 ) ||
					(code == 215999  )  ) {
				type = "ワイン";
			}

			// ★ タイプ：ウイスキー・ブランデー類
			if (  (code>=213001  && code <= 213003 )  || (code == 213005 )  ) {
				type = "ウイスキー・ブランデー類";
			}

			// ★ タイプ：スピリッツ
			if (  (code>=217001  && code <= 217004 )  || (code == 217999  )  ) {
				type = "スピリッツ";
			}

			// ★ タイプ：焼酎類
			if (csv[PosUtils.ITEM_NAME].matches(".*(焼酎).*")) {
				type = "焼酎類";
			}

			// ★ タイプ：リキュール
			if (  (code == 216002 )  || (code == 216999  )  ) {
				type = "リキュール";
			}

			// ★ タイプ：カクテルドリンク類
			if (  (code == 219003  )  || (code == 219004 ) || (code == 219009 )  ) {
				type = "カクテルドリンク類";
			}

			// ★ タイプ：中国酒
			if (  (code == 218001   )  || (code == 218999 )  ) {
				type = "中国酒";
			}

			// ★ タイプ：雑酒
			if (  (code >= 220001  && code <= 220005 && code != 220003 ) ||
				(code == 220999    )  ) {
				type = "雑酒";
			}


			// ★ タイプ：アルコールテイスト飲料類
			if ( code>=222001  && code <= 222004 ) {
				type = "アルコールテイスト飲料類";
			}

			// ★ タイプ：酒類関連飲料
			if ( code>=223001  && code <= 223003 ) {
				type = "酒類関連飲料";
			}


			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(type), new CSKV(count));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			// 個数を合計
			int count = 0;
			for (CSKV value : values) {
				count += value.toInt();
			}

			// emit
			context.write(key, new CSKV(count));
		}
	}
}
