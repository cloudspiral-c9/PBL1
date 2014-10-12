package posmining.condom;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import posmining.utils.CSKV;

public class CondomMapper2 extends Mapper<LongWritable, Text, CSKV, CSKV> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] csv = value.toString().split("\t");

		String mapKey = "condomPlusEnergyNum";
		String mapValue = csv[1];

		context.write(new CSKV(mapKey), new CSKV(mapValue));
	}
}
