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
	 * <p>
	 * 以下が具体的なmapping
	 * <ul>
	 * <li>実年でないレシートは除外
	 * <li>商品カテゴリーをキーに、レシートIDをバリューにしてemit
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
			String year = rid.split("-")[0];
			String count = csv[1];

			String mapkey = year + ", " +ConvertCategory(count);

			context.write(new CSKV(mapkey), new CSKV(0));
		}
	}

	/**
	 * カテゴリ番号をPOSデータの大分類指定に変換する関数<br/>
	 * <p>
	 * 大まかなアルゴリズムは以下の通り
	 * <ul>
	 * <li>まず、引数の長さが6桁あるかないかチェックし、なければ0を追加する<br/>
	 * <li>引数の頭文字3つを取得し、int型に変換する。<br/>
	 * <li>そこから大分類指定のものと当てはめていく<br/>
	 * </ul>
	 * 追記：日本語だとAWSの結果が文字化けするため、数字で結果を出力したあとcsvファイルを後で編集しました。<br/>
	 * 	    poscode.txtに番号とカテゴリの割り振りを乗せときます。
	 * 
	 * @see posデータ:<a href = "http://t21help.nikkei.co.jp/reference/POSCodeBook18.pdf">分類コード・分類名</a>
	 * @param category
	 * @return
	 */
	private static String ConvertCategory(String category) {

		if(category.length() == 5){
			category = "0" + category;
		}else if(category.length() == 4){
			category = "00" + category;
		}

		String scategory = category.substring(0,3);
		int posnumber = Integer.valueOf(scategory);
		category = "";

		if(posnumber>=1 && posnumber <= 3){
			category = "1";
		}else if(posnumber>=11 && posnumber <=14){
			category = "2";
		}else if (posnumber>=21 && posnumber <=29){
			category = "3";
		}else if(posnumber>=31 && posnumber <=33){
			category = "4";
		}else if(posnumber>=41 && posnumber <=50){
			category = "5";
		}else if(posnumber>=61 && posnumber <=62){
			category ="6";
		}else if(posnumber>=71 && posnumber <=85){
			category = "7";
		}else if(posnumber>=101 && posnumber <=108){
			category = "8";
		}else if(posnumber>=111 && posnumber <=128){
			category = "9";
		}else if(posnumber>=131 && posnumber <=149){
			category = "10";
		}else if(posnumber>=151 && posnumber <=159){
			category = "11";
		}else if(posnumber>=161 && posnumber <=166){
			category = "12";
		}else if(posnumber>=171 && posnumber <=176){
			category = "13";
		}else if(posnumber>=181 && posnumber <=188){
			category = "14";
		}else if(posnumber>=191 && posnumber <=206){
			category = "15";
		}else if(posnumber>=211 && posnumber <=223){
			category = "16";
		}else if(posnumber>=231 && posnumber <=236){
			category = "17";
		}else if(posnumber >=251 && posnumber <=252){
			category = "18";
		}else if(posnumber >=261  && posnumber <=263){
			category ="19";
		}else if(posnumber >= 601&& posnumber <= 670){
			category = "20";
		}else if(posnumber == 681){
			category = "21";
		}else if(posnumber >=671 && posnumber <=871 && posnumber != 681){
			category = "22";
		}else if(posnumber >=691 && posnumber <=706){
			category = "23";
		}else{
			category = "24";
		}
		return category;
	}


	/**
	 *  Reducerクラスのreduce関数を定義<br/>
	 * <p>
	 * 以下が具体的なreduce法
	 * <ul>
	 * <li>レシートIDの数をカウント
	 * <li>商品カテゴリーをキーに、カテゴリ数をバリューにしてemit
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
			//String category = key.toString().split(",")[0];

			// emit
			context.write(key,new CSKV(count));
		}
	}


}
