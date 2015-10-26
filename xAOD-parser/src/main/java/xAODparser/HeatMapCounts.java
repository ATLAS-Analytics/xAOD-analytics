package xAODparser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// receives tuple of  maps "branch":count
// returns map "branch":count

public class HeatMapCounts extends EvalFunc<Tuple> {
	private final static Logger logger = LoggerFactory.getLogger(HeatMapCounts.class);

	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			Map<String, Long> ABmap = new HashMap<String, Long>();

			DataBag bag = (DataBag) input.get(0);
			Iterator<Tuple> it = bag.iterator();
			while (it.hasNext()) {
				Tuple t = (Tuple) it.next();
				if (t==null) break;
				if (t.size()<1) break;
				Map<String, Integer> mp = (Map<String, Integer>) t.get(0);
				for (Entry<String, Integer> entry : mp.entrySet()) {
					String brn = entry.getKey();
					if (!ABmap.containsKey(brn)) {
						ABmap.put(brn, 1L);
					} else {
						Long ov = ABmap.get(brn);
						ABmap.put(brn, ov + 1);
					}
				}
			}


			 TupleFactory tupleFactory = TupleFactory.getInstance();
			 Tuple output = tupleFactory.newTuple(1);
			 output.set(0, ABmap);
			
			 return output;

		} catch (Exception e) {
			logger.warn("exception processing input row: ", e.toString());
			warn(e.toString(), PigWarning.UDF_WARNING_1);
			throw new IOException("Caught exception processing input row ", e);
		}
	}

	public Schema outputSchema(Schema input) {
		try {

			Schema fSchema = new Schema();
			Schema retSchema = new Schema();
			Schema filesSchema = new Schema();

			filesSchema.add(new Schema.FieldSchema("name", DataType.CHARARRAY));

			retSchema.add(new Schema.FieldSchema("AccessedBranches", DataType.MAP));

			fSchema.add(new Schema.FieldSchema("line", retSchema, DataType.TUPLE));

			return fSchema;

		} catch (Exception e) {
			return null;
		}
	}

}