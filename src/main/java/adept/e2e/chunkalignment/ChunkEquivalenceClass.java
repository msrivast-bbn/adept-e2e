package adept.e2e.chunkalignment;

import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import adept.common.Chunk;
import adept.common.HltContentContainer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class encapsulates all the {@link Chunk}s that are aligned to each other.<br> Along with a
 * list of all the aligned chunks, it also contains an additional list of all the aligned "pivot"
 * chunks. Pivot chunks are chunks that come from pivot {@link HltContentContainer} objects.<br>
 * Containers can be marked as pivot using {@link DefaultChunkAligner.DocumentAlignmentOptions}.<br>
 * See {@link DefaultChunkAligner#align(HltContentContainer, DefaultChunkAligner.DocumentAlignmentOptions)}
 * for an example.<br> This class is locally immutable. If any contained Chunks are altered,
 * everything is undefined
 *
 * @author mroy
 */
public final class ChunkEquivalenceClass implements IChunkEquivalenceClass, Serializable {

  private static final long serialVersionUID = 5545035490303429535L;
  // using Sets here depends on fixing the problem
  // with Chunk .hashCode and equals()
  private final ImmutableSet<Chunk> chunks;
  private final ImmutableSet<Chunk> pivots;

  private ChunkEquivalenceClass(Iterable<? extends Chunk> chunks,
      Iterable<? extends Chunk> pivots) {
    this.chunks = ImmutableSet.copyOf(chunks);
    this.pivots = ImmutableSet.copyOf(pivots);
    checkArgument(this.chunks.containsAll(this.pivots), "Pivots must be a subset of chunks");
  }

  public static ChunkEquivalenceClass from(Iterable<? extends Chunk> chunks,
      Iterable<? extends Chunk> pivots) {
    return new ChunkEquivalenceClass(chunks, pivots);
  }

  @Override
  public Iterator<Chunk> iterator() {
    return chunks.iterator();
  }

  @Override
  public Collection<Chunk> chunks() {
    return chunks;
  }

  @Override
  public Collection<Chunk> pivots() {
    return pivots;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ChunkEquivalenceClass chunks1 = (ChunkEquivalenceClass) o;

    if (!chunks.equals(chunks1.chunks)) {
      return false;
    }
    return pivots.equals(chunks1.pivots);
  }

  @Override
  public int hashCode() {
    int result = chunks.hashCode();
    result = 31 * result + pivots.hashCode();
    return result;
  }
}
