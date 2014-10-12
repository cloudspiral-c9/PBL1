package posmining.condom;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import posmining.utils.CSKV;

public class CondomOnlyReducer2 extends Reducer<CSKV, CSKV, CSKV, CSKV> {

	@Override
	protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

		long count = 0;
		for (CSKV value : values) {
			count += value.toLong();
		}

		context.write(new CSKV("comdomNum") , new CSKV(count));
	}

}
