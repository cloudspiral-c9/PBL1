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

		String[] csv = value.toString().split("\t");

		String receiptId = csv[0];
		String ageFlg = csv[1];

		String year = receiptId.split("-")[0];
		String mapKey = year + ", " +ageFlgConvert(ageFlg);

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
				retString = "子供";
				break;

			case 2:
				retString = "若者";
				break;

			case 3:
				retString = "大人";
				break;

			case 4:
				retString = "実年";
				break;

			default:
				retString = "";
				break;
		}

		return retString;
	}
}
