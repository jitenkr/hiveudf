package jk.hive2.hiveudf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class TransactionParserGenericUDTF extends GenericUDTF{
	private PrimitiveObjectInspector stringOI = null;
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 9) {
			throw new UDFArgumentException("TransactionParserGenericUDTF() takes exactly 9 argument");
		}
		for (int i=0;i<9;i++) {
			if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) args[i])
					.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
				throw new UDFArgumentException("TransactionParserGenericUDTF("+i+") takes a string as a parameter");
			}
		}
		// input
		stringOI = (PrimitiveObjectInspector) args[0];

		// output
		List<String> fieldNames = new ArrayList<String>(2);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
		fieldNames.add("name");
		fieldNames.add("surname");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}
	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(Object[] arg0) throws HiveException {
		// TODO Auto-generated method stub
		
	}

}
