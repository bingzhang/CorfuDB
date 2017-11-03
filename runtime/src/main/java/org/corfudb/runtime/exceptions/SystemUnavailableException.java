package org.corfudb.runtime.exceptions;

/**
 * Created by rmichoud on 10/31/17.
 */
public class SystemUnavailableException extends RuntimeException {
    public SystemUnavailableException(String reason) {
        super(reason);
    }

}
