/**
 * PartitioningError.java
 * 
 * An exception occurred during the partitioning.
 * 
 * @author John Koumarelas
 */
package datatypes.exceptions;

public class PartitioningError extends Exception {
	
	private static final long serialVersionUID = 3244247624881212622L;

	public PartitioningError(String message) {
        super(message);
    }	
	
}
