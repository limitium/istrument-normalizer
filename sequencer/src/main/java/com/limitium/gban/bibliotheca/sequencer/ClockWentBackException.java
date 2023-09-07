package com.limitium.gban.bibliotheca.sequencer;

public class ClockWentBackException extends RuntimeException{
    public ClockWentBackException(String message) {
        super(message);
    }
}
