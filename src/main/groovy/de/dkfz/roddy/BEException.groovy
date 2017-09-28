package de.dkfz.roddy

/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

class BEException extends Exception {

    BEException(String message) {
        super(message)
    }

    BEException(String message, Throwable cause) {
        super(message, cause)
    }

    BEException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace)
    }

    BEException(Throwable cause) {
        super(cause)
    }

}
