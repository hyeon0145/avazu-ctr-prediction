package trainer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;

public class Example implements WritableComparable<Example> {
	private HashMap<Integer, Integer> X;
	private int y;
	
	public Example() {
		this.X = new HashMap<Integer, Integer>();
		this.y = 0;
	}
	
	public HashMap<Integer, Integer> getX() {
		return this.X;
	}
	
	public int getY() {
		return this.y;
	}
	
	public void setY(int y) {
		this.y = y;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		this.X = new HashMap<Integer, Integer>();
		for (int i = 0; i < size; ++i) {
			int key = in.readInt();
			int value = in.readInt();
			this.X.put(key, value); 
		}
		
		this.y = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.X.size());
		for (Entry<Integer, Integer> entry : this.X.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());
		}
		
		out.writeInt(this.y);
	}

	@Override
	public int compareTo(Example example) {
		if (this.X.size() != example.X.size()) {
			return Integer.compare(this.X.size(), example.X.size());
		}
		
		if (this.y != example.y) {
			return Integer.compare(this.y, example.y);
		}
		
		for (Entry<Integer, Integer> entry : this.X.entrySet()) {
			if (!example.X.containsKey(entry.getKey())) {
				return 1;
			}
			
			if (entry.getValue() != example.X.get(entry.getKey())) {
				return Integer.compare(entry.getValue(), example.X.get(entry.getKey()));
			}
		}
		
		return 0;
	}
}
