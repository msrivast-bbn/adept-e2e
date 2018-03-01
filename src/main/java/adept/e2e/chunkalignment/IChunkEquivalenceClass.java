package adept.e2e.chunkalignment;

import java.util.Collection;

import adept.common.Chunk;

/**
 * non-API visible super-interface of both mutable and immutable ChunkEquivalenceClasses
 *
 * @author mroy
 */
public interface IChunkEquivalenceClass extends Iterable<Chunk> {

  Collection<Chunk> chunks();

  Collection<Chunk> pivots();
}
