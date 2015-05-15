package org.idryman.aggregation;

/**
 * A Accumulator key must implement the accumulate method,
 * which reads another key of the same type and accumulates
 * each of its fields.
 * 
 * The key should also implement the reset method.
 * having a reset is better than destroy and re-create
 * a new object for every iteration.
 * @author felix.chern
 *
 * @param <T>
 */
public interface Accumulator<T> {
  void accumulate(T other);
  void reset();
}
