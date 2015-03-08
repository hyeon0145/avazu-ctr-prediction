package predictor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class Predictor {
	public static void main(String[] arguments) throws IOException {
		if (arguments.length != 3) {
			System.err.println("Parameter: <number of bins> <theta file> <predict file>");
			System.exit(1);
		}
		
		int numberOfBins = Integer.parseInt(arguments[0]);
		double[] theta = Predictor.readTheta(arguments[1], numberOfBins);
		
		BufferedReader reader = new BufferedReader(new FileReader(arguments[2]));
		try {
			String[] header = reader.readLine().trim().split(",");
			
			String line = reader.readLine();
			while (line != null) {
				String[] items = line.trim().split(",");
				HashMap<String, String> data = new HashMap<String, String>();
				
				for (int i = 0; i < items.length; ++i) {
					data.put(header[i], items[i]);
				}
				
				HashMap<Integer, Integer> X = Predictor.applyFeatureHashing(data, numberOfBins);
				double h = Predictor.h(theta, X);
				System.out.println((h >= 0.5) ? '1' : '0');
				
				line = reader.readLine();
			}
		} finally {
			reader.close();
		}
	}
	
	private static double[] readTheta(String filename, int numberOfBins) throws IOException {
		double[] theta = new double[numberOfBins + 1];
		
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		try {	
			String[] items = reader.readLine().trim().split("\t");
			
			for (int i = 0; i < items.length; ++i) {
				theta[i] = Double.parseDouble(items[i]);
			}
		} finally {
			reader.close();
		}
		
		return theta;
	}
	
	private static HashMap<Integer, Integer> applyFeatureHashing(HashMap<String, String> data, int numberOfBins) {
		HashMap<Integer, Integer> X = new HashMap<Integer, Integer>();
		X.put(0, 1);
		
		data.remove("id");
		data.remove("click");
		
		data.put("hour", data.get("hour").substring(6)); // use  only *real hour*

		// applying feature hashing
		for (Entry<String, String> entry : data.entrySet()) {
			int hash = (entry.getKey() + entry.getValue()).hashCode();
			int index = hash % numberOfBins; // 0 <= index < this.numberOfBins
			if (index < 0) {
				index += numberOfBins;
			}
			index += 1; // 1 <= index < this.numberOfBins + 1
			
			if (X.containsKey(index)) {
				X.put(index, X.get(index) + 1);
			} else {
				X.put(index, 1);
			}
		}
		
		return X;
	}
	
	private static double h(double theta[], HashMap<Integer, Integer> X) {
		double xTheta = 0.0;
		for (Entry<Integer, Integer> entry : X.entrySet()) {
			xTheta += entry.getValue() * theta[entry.getKey()];
		}
		
		return Predictor.g(xTheta);
	}
	
	private static double g(double z) {
		// to prevent overflow -> -50 <= z <= 50
		z = Math.max(z, -50.0);
		z = Math.min(z, 50.0);
		
		return 1.0 / (1.0 + Math.exp(-z)); 
	}
}
