package posmining.condom;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import posmining.utils.CSKV;

public class CondomOnlyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {


	@Override
	protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

		boolean condomFlg = false;
		boolean energyDrinkFlg = true;

		for (CSKV value : values) {

			if (value.toString().equals("コンドーム・スキン")) {
				condomFlg = true;
			}

			if (value.toString().equals("栄養補給ドリンク")) {
				energyDrinkFlg = true;
			}
		}

		if (condomFlg && energyDrinkFlg) {
			context.write(key, new CSKV(1));
		}

	}
}
