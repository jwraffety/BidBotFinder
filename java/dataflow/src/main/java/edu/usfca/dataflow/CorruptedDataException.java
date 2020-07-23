package edu.usfca.dataflow;

/**
 * @author Jackson
 * The below courtesy of Hayden Lee of University of San Francisco.
 * Can be useful for debugging.
 */

public class CorruptedDataException extends IllegalArgumentException {

  public CorruptedDataException() {
    super();
  }

  public CorruptedDataException(String s) {
    super(s);
  }

  public CorruptedDataException(String message, Throwable cause) {
    super(message, cause);
  }

  public CorruptedDataException(Throwable cause) {
    super(cause);
  }

  private static final long serialVersionUID = -686L;
}
