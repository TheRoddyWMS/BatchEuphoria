/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy

/**
 * A collection of messages, errors and other constant strings.
 * Created by heinold on 24.02.17.
 */
class Constants {

    /////////////////////////
    // Error messages
    /////////////////////////

    public static final String ERR_MSG_ONLY_ONE_JOB_ALLOWED = "A job object is not allowed to run several times.";
    public static final String ERR_MSG_WRONG_PARAMETER_COUNT = "You did not provide proper parameters, args.length = ";
    public static final String ERR_MSG_NO_APPLICATION_PROPERTY_FILE = "Configuration does not exist. Cannot start application.";
}
