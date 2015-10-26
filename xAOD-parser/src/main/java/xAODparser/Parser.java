package xAODparser;

import org.json.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ip:chararray,timeentry:long,ROOT_RELEASE:chararray,ReadCalls:int,ReadSize:int,CacheSize:int, PandaID:long, PanDA_TaskID:int
// accessedBranches[{"et_x":234},{}], accessedContainers[{"cont":234},{}], accessedFiles["filename","filename"], 
// fileType:chararray, storageType:chararray

public class Parser extends EvalFunc<Tuple> {
	private final static Logger logger = LoggerFactory.getLogger(Parser.class);

	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			String str = (String) input.get(0);
			warn("input string:" + str, PigWarning.UDF_WARNING_1);

			JSONObject obj = new JSONObject(str);
			warn("json string:" + obj.toString(), PigWarning.UDF_WARNING_2);

			TupleFactory tupleFactory = TupleFactory.getInstance();

			JSONArray jAF = obj.getJSONArray("accessedFiles");
			JSONArray jAB = obj.getJSONArray("accessedBranches");
			JSONArray jAC = obj.getJSONArray("accessedContainers");

			String fileType = "not Sure";
			String storageType = "local";

			Tuple AFtuple = tupleFactory.newTuple(jAF.length());
			for (int i = 0; i < jAF.length(); i++) {
				String fn = jAF.getString(i);
				AFtuple.set(i, fn);
				if (i > 0)
					continue;
				try {
					if (fn.contains("/AOD") || fn.contains(".AOD.") || fn.contains(":AOD"))
						fileType = "AOD";
					else if (fn.contains("/ESD") || fn.contains(".ESD.")  || fn.contains(":ESD"))
						fileType = "ESD";
					else if (fn.contains("/DAOD_")) {
						int st = fn.indexOf("/DAOD_");
						int en = fn.indexOf(".", st + 2);
						int en1 = fn.indexOf("/", st + 2);
						if (en1 > 0 && en1 < en)
							en = en1;
						if (en > st + 1)
							fileType = fn.substring(st + 1, en);
					} else if (fn.contains(":DAOD_")) {
						int st = fn.indexOf(":DAOD_");
						int en = fn.indexOf(".", st + 3);
						int en1 = fn.indexOf("/", st + 3);
						if (en1 > 0 && en1 < en)
							en = en1;
						if (en > st + 1)
							fileType = fn.substring(st + 1, en);
					}

					if (fn.startsWith("root://"))
						storageType = "xrootd";
					else if (fn.startsWith("dcap://"))
						storageType = "dcap";
				} catch (IndexOutOfBoundsException e) {
					log.error("could not determine fileType" + fn);
				}
			}

			Map<String, Integer> ABmap = new HashMap<String, Integer>();
			for (int i = 0; i < jAB.length(); i++) {
				JSONObject jO = jAB.getJSONObject(i);
				Iterator<?> keys = jO.keys();
				while (keys.hasNext()) {
					String key = (String) keys.next();
					ABmap.put(key, jO.getInt(key));
				}
			}

			Map<String, Integer> ACmap = new HashMap<String, Integer>();
			for (int i = 0; i < jAC.length(); i++) {
				JSONObject jO = jAC.getJSONObject(i);
				Iterator<?> keys = jO.keys();
				while (keys.hasNext()) {
					String key = (String) keys.next();
					ACmap.put(key, jO.getInt(key));
				}
			}

			Tuple output = tupleFactory.newTuple(12);
			output.set(0, obj.optLong("PandaID"));
			output.set(1, obj.optLong("PanDA_TaskID"));
			output.set(2, obj.getString("ip"));
			output.set(3, obj.getString("ROOT_RELEASE"));
			output.set(4, obj.getLong("ReadCalls"));
			output.set(5, obj.getLong("ReadSize"));
			output.set(6, obj.getLong("CacheSize"));
			output.set(7, AFtuple);
			output.set(8, ABmap);
			output.set(9, ACmap);
			output.set(10, fileType);
			output.set(11, storageType);

			return output;
		} catch (JSONException e) {
			logger.warn("could not parse row: ", e.toString());
			// throw new
			// IOException("Caught JSON parsing exception processing input row ",
			// e);
			return null;
		} catch (Exception e) {
			logger.warn("exception processing input row: ", e.toString());
			throw new IOException("Caught exception processing input row ", e);
		}
	}

	public Schema outputSchema(Schema input) {
		try {

			Schema fSchema = new Schema();
			Schema retSchema = new Schema();
			// Schema inputFilesSchema = new Schema();
			Schema filesSchema = new Schema();

			filesSchema.add(new Schema.FieldSchema("name", DataType.CHARARRAY));

			// inputFilesSchema.add(new Schema.FieldSchema("inpFiles",
			// filesSchema, DataType.TUPLE));

			retSchema.add(new Schema.FieldSchema("PandaID", DataType.LONG));
			retSchema.add(new Schema.FieldSchema("TaskID", DataType.LONG));
			retSchema.add(new Schema.FieldSchema("IP", DataType.CHARARRAY));
			retSchema.add(new Schema.FieldSchema("ROOT_RELEASE", DataType.CHARARRAY));
			retSchema.add(new Schema.FieldSchema("ReadCalls", DataType.LONG));
			retSchema.add(new Schema.FieldSchema("ReadSize", DataType.LONG));
			retSchema.add(new Schema.FieldSchema("CacheSize", DataType.LONG));
			retSchema.add(new Schema.FieldSchema("accessedFiles", filesSchema, DataType.TUPLE));
			retSchema.add(new Schema.FieldSchema("AccessedBranches", DataType.MAP));
			retSchema.add(new Schema.FieldSchema("AccessedContainers", DataType.MAP));
			retSchema.add(new Schema.FieldSchema("fileType", DataType.CHARARRAY));
			retSchema.add(new Schema.FieldSchema("storageType", DataType.CHARARRAY));

			fSchema.add(new Schema.FieldSchema("line", retSchema, DataType.TUPLE));

			return fSchema;

		} catch (Exception e) {
			return null;
		}
	}

}