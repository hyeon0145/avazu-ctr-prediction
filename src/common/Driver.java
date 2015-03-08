package common;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {
	public static void main(String[] arguments) {
		ProgramDriver driver = new ProgramDriver();
		try {
			driver.addClass("train", trainer.Trainer.class, "");
			driver.driver(arguments);
		} catch (Throwable exception) {
			exception.printStackTrace();
		}
	}
}
