package com.limitium.gban.bibliotheca.sequencer;

public class ClockStuckException extends RuntimeException{
    public ClockStuckException(String message) {
        super(message);
    }
}
