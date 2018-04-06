package com.gslab.pepper.exception;

public class PepperBoxException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = -1046606884335147045L;

	public PepperBoxException(String message) {
        super(message);
    }

    public PepperBoxException(Exception exc) {
        super(exc);
    }

}