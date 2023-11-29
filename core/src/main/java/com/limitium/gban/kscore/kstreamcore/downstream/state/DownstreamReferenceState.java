package com.limitium.gban.kscore.kstreamcore.downstream.state;

public class DownstreamReferenceState {
    public ReferenceState state;
    public long effectiveReferenceId;
    public int effectiveReferenceVersion;

    public DownstreamReferenceState(ReferenceState state, long effectiveReferenceId, int effectiveReferenceVersion) {
        this.state = state;
        this.effectiveReferenceId = effectiveReferenceId;
        this.effectiveReferenceVersion = effectiveReferenceVersion;
    }

    @Override
    public String toString() {
        return "DownstreamReferenceState{" +
                "state=" + state +
                ", effectiveReferenceId=" + effectiveReferenceId +
                ", effectiveReferenceVersion=" + effectiveReferenceVersion +
                '}';
    }

    public enum ReferenceState {
        UNAWARE, EXISTS, CANCELED
    }
}
