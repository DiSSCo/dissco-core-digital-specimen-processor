package eu.dissco.core.digitalspecimenprocessor.exception;

public class DisscoRepositoryException extends Exception{

  public DisscoRepositoryException(String message) {
    super(message);
  }

  public DisscoRepositoryException(String message, Throwable cause) {
    super(message, cause);
  }
}
