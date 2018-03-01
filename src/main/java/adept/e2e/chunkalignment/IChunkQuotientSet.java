package adept.e2e.chunkalignment;

import com.google.common.base.Optional;

import java.util.Collection;

import adept.common.Chunk;

/**
 * Super-interface of both mutable and immutable chunk quotient sets.
 *
 * @author mroy
 */
public interface IChunkQuotientSet<T extends IChunkEquivalenceClass> extends Iterable<T> {

  boolean areEquivalent(Chunk c1, Chunk c2);

  // Optional because the chunk may be totally unaligned...
  Optional<T> equivalenceClassContaining(Chunk c);

  Collection<T> equivalenceClasses();

  // we might consider offering the following, where Equivalence is from
  // http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/base/Equivalence.html
  //Equivalence<Chunk> asEquivalence();
}
