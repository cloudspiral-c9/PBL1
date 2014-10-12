package posmining.age;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import posmining.utils.CSKV;

public class AgeReducer2 extends Reducer<CSKV, CSKV, CSKV, CSKV> {

	@Override
	protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		for (CSKV value : values) {
			count+= value.toInt();
		}
		context.write(key, new CSKV(count));
	}


}
