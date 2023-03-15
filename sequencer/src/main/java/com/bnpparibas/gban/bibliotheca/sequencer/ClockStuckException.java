package com.bnpparibas.gban.bibliotheca.sequencer;

public class ClockStuckException extends RuntimeException{
    public ClockStuckException(String message) {
        super(message);
    }
}
