package posmining.age;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import posmining.utils.CSKV;

/**
 * Reducer
 * @author Seiya
 *
 */
public class AgeReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {

	@Override
	protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

		int ageFlg = 0;
		for (CSKV value : values) {
			ageFlg = value.toInt();
			break;
		}
		context.write(key, new CSKV(ageFlg));
	}
}
