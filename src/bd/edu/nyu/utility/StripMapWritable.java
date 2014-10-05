
package bd.edu.nyu.utility;

/*import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;*/
import java.io.DataInput;
/*import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
*/import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
/*
 writable  extending MapWritable*/
public class StripMapWritable extends MapWritable {

		public StripMapWritable() {
		super();
	}


	public static StripMapWritable create(DataInput in) throws IOException {
		StripMapWritable m = new StripMapWritable();
		m.readFields(in);

		return m;
	}

	public void plus(StripMapWritable m) {
		for (Map.Entry<Writable, Writable> e : m.entrySet()) {
			Text key = (Text)e.getKey();
			
			if (this.containsKey(key)) {
				IntWritable val1 = new IntWritable(0);
				val1 = (IntWritable)(this.get(key));
				IntWritable val2 = new IntWritable(0);
				val2 = (IntWritable)(e.getValue());
				int val = val1.get() + val2.get();
				val1.set(val);
				this.put(key,val1);
			} else {
				this.put(key, e.getValue());
			}
		}
	}

//to increment the value 
	public void increment(Text key) {
	  increment(key, 1);
	}


  public void increment(Text key, int n) {
	IntWritable val1 = new IntWritable(0);
    if (this.containsKey(key)) {
    	val1 = (IntWritable)(this.get(key));
		n += val1.get();
		val1.set(n);
      this.put(key, val1);
    } else {
    	val1.set(n);
      this.put(key, val1);
    }
  }
  
  
}
