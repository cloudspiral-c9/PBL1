package posmining.age;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

/**
 * 年別，年フラグ別にレシート数を集計
 * @author Seiya
 *
 */
public class AgeMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] csv = value.toString().split(",");

		String mapKey;
		String mapValue;

		try {
			mapKey = csv[PosUtils.RECEIPT_ID];
			mapValue = csv[PosUtils.BUYER_AGE];
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		context.write(new CSKV(mapKey), new CSKV(mapValue));
	}

}
