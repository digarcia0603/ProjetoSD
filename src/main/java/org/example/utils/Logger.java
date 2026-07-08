package org.example.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {

    public enum LogLevel {
        INFO, WARN, ERROR, DEBUG
    }

    public static void log (String message, LogLevel level) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = sdf.format(new Date());

        System.out.println(String.format("[%s] [%s] %s", timestamp, level, message));
    }
}