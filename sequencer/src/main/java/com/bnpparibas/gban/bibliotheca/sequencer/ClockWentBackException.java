package com.bnpparibas.gban.bibliotheca.sequencer;

public class ClockWentBackException extends RuntimeException{
    public ClockWentBackException(String message) {
        super(message);
    }
}
