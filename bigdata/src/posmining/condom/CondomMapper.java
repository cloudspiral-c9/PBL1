package posmining.condom;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

public class CondomMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] csv = value.toString().split(",");

		String categoryName = csv[PosUtils.ITEM_CATEGORY_NAME];
		if (!categoryName.equals("コンドーム・スキン") && !categoryName.equals("栄養補給ドリンク")) {
			return;
		}

		String mapKey = csv[PosUtils.RECEIPT_ID];
		String mapValue = categoryName;

		context.write(new CSKV(mapKey), new CSKV(mapValue));
	}
}
