package org.example;

import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
import io.opentelemetry.sdk.trace.IdGenerator;
import javax.annotation.Nonnull;

/**
 * Custom {@link IdGenerator} which provides span and trace ids.
 *
 * @see io.opentelemetry.sdk.trace.SdkTracerProvider
 */
public class SequencedIdGenerator implements IdGenerator {
  private final Sequencer sequencer;

  public SequencedIdGenerator(@Nonnull String serviceName, @Nonnull String instanceName) {
    int serviceId = serviceName.hashCode() % (2 << (Sequencer.NAMESPACE_BITS - 1));
    int instanceId = instanceName.hashCode() % (2 << (Sequencer.PARTITION_BITS - 1));

    sequencer = new Sequencer(System::currentTimeMillis, serviceId, instanceId);
  }

  @Override
  public synchronized String generateSpanId() {
    return String.format("%016d", sequencer.getNext() % 10000000000000000L);
  }

  @Override
  public synchronized String generateTraceId() {
    return String.format("%032d", sequencer.getNext());
  }
}
