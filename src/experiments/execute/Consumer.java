/**
 * Consumer.java
 * 
 * Executes a set of parameters, by calling the appropriate executor. 
 * 
 * @author John Koumarelas
 */

package experiments.execute;

import java.io.FileWriter;
import control.Controller;

public class Consumer extends Thread{

	private String parameters;
	private FileWriter out;
	
	public Consumer(String parameters, FileWriter out) {
		this.parameters = parameters;
		this.out = out;
	}
	
	@Override
	public void run() {
		
		String[] args = parameters.split("\t");
		try {
			Controller.main(args);
			System.out.println("execution OK: " + parameters);
			synchronized(out) {
				out.write(parameters + "\n");
			}
		}
		catch (Exception e) {
			System.out.println("execution EXCEPTION: " + parameters);
			e.printStackTrace();
		}
	}
}
