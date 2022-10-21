package com.bnpparibas.gban.usstreetcontroller.common.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TigerAllocation {
    static Logger logger = LoggerFactory.getLogger(State.class);
    public long id;
    public long executionId;
    public State state;
    public long version;

public enum State {
        NEW_PENDING {
            public State transferTo(State state) throws WrongStateTransitionException {
                if (state == NEW_NACK) {
                    return NEW_NACK;
                }
                if (state == NEW_ACK) {
                    return NEW_ACK;
                }
                if (state == CANCEL_PENDING) {
                    return CANCEL_PENDING;
                }
                String msg = "Wrong state transfer from NEW_ACK to:" + state;
                logger.error(msg, state);
                throw new WrongStateTransitionException(msg);
            }
        },
        NEW_ACK {
            public State transferTo(State state) throws WrongStateTransitionException {
                if (state == CANCEL_PENDING) {
                    return CANCEL_PENDING;
                }
                String msg = "Wrong state transfer from NEW_ACK to:" + state;
                logger.error(msg, state);
                throw new WrongStateTransitionException(msg);
            }
        },
        NEW_NACK {
            public State transferTo(State state) throws WrongStateTransitionException {
                String msg = "Try to change terminated state NEW_NACK to:" + state;
                logger.error(msg, state);
                throw new WrongStateTransitionException(msg);
            }
        },
        CANCEL_PENDING {
            public State transferTo(State state) throws WrongStateTransitionException {
                if (state == NEW_NACK) {
                    return NEW_NACK;
                }
                if (state == NEW_ACK) {
                    return CANCEL_PENDING;
                }
                if (state == CANCELED) {
                    return CANCELED;
                }
                String msg = "Wrong state transfer from CANCEL_PENDING to:" + state;
                logger.error(msg, state);
                throw new WrongStateTransitionException(msg);
            }
        },
        CANCELED {
            public State transferTo(State state) throws WrongStateTransitionException {
                String msg = "Try to change terminated state CANCEL to:" + state;
                logger.error(msg, state);
                throw new WrongStateTransitionException(msg);
            }
        };

        abstract public State transferTo(State state) throws WrongStateTransitionException;

        public static class WrongStateTransitionException extends Exception {
            public WrongStateTransitionException(String msg) {
                super(msg);
            }
        }
    }
}
