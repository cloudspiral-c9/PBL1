package posmining.age;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

public class AgeMapper2 extends Mapper<LongWritable, Text, CSKV, CSKV> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println("string: " + value.toString());
		String[] csv = value.toString().split("\t");

		System.out.println("csvLength: " + csv.length);
		String receiptId = csv[0];
		String ageFlg = csv[1];

		String year = receiptId.split("-")[0];
		String flg = ageFlgConvert(ageFlg);
		String mapKey = year + ", " + ageFlgConvert(ageFlg);

		context.write(new CSKV(mapKey), new CSKV(1));
	}


	/**
	 * 年代を表す言葉に変換します．
	 * @param ageFlg
	 * @return
	 */
	private String ageFlgConvert(String ageFlg) {

		int ageFlgInt = Integer.parseInt(ageFlg);
		String retString = "";

		switch (ageFlgInt) {

			case 1:
				retString = "child";
				break;

			case 2:
				retString = "young";
				break;

			case 3:
				retString = "adult";
				break;

			case 4:
				retString = "senior";
				break;

			default:
				retString = "";
				break;
		}

		return retString;
	}
}
