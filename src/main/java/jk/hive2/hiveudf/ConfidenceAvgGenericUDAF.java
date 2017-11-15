package jk.hive2.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class ConfidenceAvgGenericUDAF extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length != 1) {
			throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
		}

		ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);

		if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Argument must be PRIMITIVE, but " + oi.getCategory().name() + " was passed.");
		}

		PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;

		if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentTypeException(0,
					"Argument must be String, but " + inputOI.getPrimitiveCategory().name() + " was passed.");
		}

		return new AverageUDAFEvaluator();
	}

	public static class AverageUDAFEvaluator extends GenericUDAFEvaluator {
		PrimitiveObjectInspector inputOI;
		ObjectInspector outputOI;
		PrimitiveObjectInspector integerOI;
		double total = 0.0d;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			assert (parameters.length == 1);
			super.init(m, parameters);

			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
			} else {
				integerOI = (PrimitiveObjectInspector) parameters[0];
			}
			// init output object inspectors
			// For partial function - array of integers
			outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Double.class,
					org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			return outputOI;
		}

		@SuppressWarnings("deprecation")
		static class ConfidenceSumAggBuffer implements AggregationBuffer {
			double sum = 0.0d;

			void add(double num) {
				sum += num;
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			// TODO Auto-generated method stub
			ConfidenceSumAggBuffer confBuffer = new ConfidenceSumAggBuffer();
			return confBuffer;
		}

		private boolean warned = false;

		@SuppressWarnings("deprecation")
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			assert (parameters.length == 1);
			if (parameters[0] != null) {
				ConfidenceSumAggBuffer myagg = (ConfidenceSumAggBuffer) agg;
				Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
				myagg.add(String.valueOf(p1).length());
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {

				ConfidenceSumAggBuffer myagg1 = (ConfidenceSumAggBuffer) agg;

				Double partialSum = (Double) integerOI.getPrimitiveJavaObject(partial);

				ConfidenceSumAggBuffer myagg2 = new ConfidenceSumAggBuffer();

				myagg2.add(partialSum);
				myagg1.add(myagg2.sum);
			}

		}

		@SuppressWarnings({ "deprecation", "unused" })
		@Override
		public void reset(AggregationBuffer arg0) throws HiveException {
			ConfidenceSumAggBuffer confBuffer = new ConfidenceSumAggBuffer();

		}

		@SuppressWarnings("deprecation")
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			ConfidenceSumAggBuffer myagg = (ConfidenceSumAggBuffer) agg;
			total = myagg.sum;
			return myagg.sum;
		}

		@SuppressWarnings("deprecation")
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			ConfidenceSumAggBuffer myagg = (ConfidenceSumAggBuffer) agg;
			total += myagg.sum;
			return total;
		}

	}
}
