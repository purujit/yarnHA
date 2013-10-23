package org.apache.hadoop.yarn.server.resourcemanager.state;

import org.apache.hadoop.yarn.exceptions.YarnException;

public class RMStateException extends Exception {

	private static final long serialVersionUID = -6215086565306340109L;

	public YarnException ToYarnException() {
		return new YarnException("");
	}

}
