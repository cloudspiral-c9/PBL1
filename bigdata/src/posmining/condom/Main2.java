package posmining.condom;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.vafer.jdeb.ant.Mapper;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

public class Main2 {

	private static final String JOBNAME = "2014012";
	private static final String INPUTPATH = "out/condomPlusEnergy1st";
	private static final String OUTPUTPATH = "out/condomPlusEnergy2nd";

	private static final Class JARCLASS = Main2.class;
	private static final Class MAPPERCLASS = CondomMapper2.class;
	private static final Class REDUCERCLASS = CondomReducer2.class;

	/**
	 * ここでMapReduceを実行
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Job job = new Job(new Configuration());
		job.setJarByClass(JARCLASS);

		//入出力形式の指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);


		//アウトプットのキーバリューの型をそれぞれ設定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		//インプットファイルのパスを指定
		FileInputFormat.setInputPaths(job, new Path(INPUTPATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUTPATH));


		//いったんアウトプットパスを削除して，ロック状態を解放しておく
		PosUtils.deleteOutputDir(OUTPUTPATH);


		//タスク数の設定
		job.setNumReduceTasks(8);


		//MapperとReducerの設定
		job.setJobName(JOBNAME);
		job.setMapperClass(MAPPERCLASS);
		job.setReducerClass(REDUCERCLASS);

		job.waitForCompletion(true);
	}
}
