package adept.e2e.chunkalignment;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import adept.common.Chunk;
import adept.common.EntityMention;
import adept.common.TokenOffset;
import adept.e2e.driver.E2eConstants;

import static adept.e2e.driver.E2eConstants.LANGUAGE;

/**
 * The set of all (disjoint) ChunkEquivalenceClasses.<br> Locally immutable (Chunks in Adept are
 * mutable, so it can't be truly immutable).<br> If any contained Chunk is modified, the results of
 * all methods become undefined.
 *
 * @author mroy
 */
public final class ChunkQuotientSet
    implements IChunkQuotientSet<ChunkEquivalenceClass>, Serializable {
  private static final long serialVersionUID = -7685788764873950174L;
  
  private static final Map<LANGUAGE, String> determinersByLanguage;
  private static final Map<LANGUAGE, String> prepositionsByLanguage;
  static
  {
    determinersByLanguage = new HashMap<LANGUAGE,String>();
    determinersByLanguage.put(LANGUAGE.EN,
        "(\\ba\\s|\\ban\\s|\\bthe\\s|\\bthis\\s|\\bthat\\s|\\bher\\s|\\bhis\\s|\\bits\\s|\\banother\\s|\\beach\\s|\\bevery\\s|\\btheir\\s|\\bour\\s)");
    determinersByLanguage.put(LANGUAGE.ZH,
        "(\\b一切\\s|\\b上\\s|\\b下\\s|\\b什么\\s|\\b任何\\s|\\b何\\s|\\b全\\s|\\b全体\\s|\\b全部\\s|\\b其他\\s|\\b其余\\s|\\b别\\s|\\b前\\s|\\b历\\s|\\b各\\s|\\b同\\s|\\b所有\\s|\\b整个\\s|\\b有些\\s|\\b本\\s|\\b某\\s|\\b此\\s|\\b每\\s|\\b该\\s|\\b这\\s|\\b这些\\s|\\b那\\s|\\b那些\\s)");
    
    prepositionsByLanguage = new HashMap<LANGUAGE,String>();
    prepositionsByLanguage.put(LANGUAGE.EN,
        "(.\\babout\\s|.\\babove\\s|.\\bacross\\s|.\\bafter\\s|.\\bagainst\\s|.\\balong\\s|.\\bamong\\s|.\\baround\\s|.\\bat\\s|.\\bbefore\\s|.\\bbehind\\s|.\\bbelow\\s|.\\bbeneath\\s|.\\bbeside\\s|.\\bbetween\\s|.\\bby\\s|.\\bdown\\s|.\\bduring\\s|.\\bexcept\\s|.\\bfor\\s|.\\bfrom\\s|.\\bin\\s|.\\binside\\s|.\\binto\\s|.\\blike\\s|.\\bnear\\s|.\\bof\\s|.\\boff\\s|.\\bon\\s|.\\bonto\\s|.\\boutside\\s|.\\bover\\s|.\\bpast\\s|.\\bsince\\s|.\\bthrough\\s|.\\bto\\s|.\\btoward\\s|.\\bunder\\s|.\\bunderneath\\s|.\\buntil\\s|.\\bup\\s|.\\bupon\\s|.\\bwith\\s|.\\bwithin\\s|.\\bwithout\\s|.\\band\\s|.\\bor\\s)");
    prepositionsByLanguage.put(LANGUAGE.ZH,
        "(\\b与\\s|\\b为\\s|\\b为了\\s|\\b于\\s|\\b从\\s|\\b以\\s|\\b作为\\s|\\b依据\\s|\\b依照\\s|\\b像\\s|\\b关于\\s|\\b到\\s|\\b同\\s|\\b向\\s|\\b和\\s|\\b因\\s|\\b因为\\s|\\b在\\s|\\b如同\\s|\\b对\\s|\\b对于\\s|\\b就\\s|\\b当\\s|\\b截至\\s|\\b按\\s|\\b按照\\s|\\b据\\s|\\b有关\\s|\\b本着\\s|\\b根据\\s|\\b比\\s|\\b用\\s|\\b由\\s|\\b由于\\s|\\b等\\s|\\b经\\s|\\b经由\\s|\\b经过\\s|\\b给\\s|\\b继\\s|\\b自\\s|\\b至\\s|\\b较\\s|\\b通过\\s|\\b遵照\\s|\\b针对\\s|\\b除\\s|\\b除了\\s|\\b随着\\s|\\b面对\\s|\\b上\\s|\\b下\\s|\\b中\\s|\\b为止\\s|\\b之下\\s|\\b之中\\s|\\b之前\\s|\\b之后\\s|\\b之间\\s|\\b之际\\s|\\b以上\\s|\\b以下\\s|\\b以外\\s|\\b以来\\s|\\b内\\s|\\b初\\s|\\b前\\s|\\b前后\\s|\\b后\\s|\\b在内\\s|\\b外\\s|\\b开始\\s|\\b旁\\s|\\b时\\s|\\b来\\s|\\b起\\s|\\b里\\s|\\b间\\s|\\b面前\\s)");
  }
  
  private E2eConstants.LANGUAGE language;
  private Collection<ChunkEquivalenceClass> mEquivalenceClasses;

  public ChunkQuotientSet(E2eConstants.LANGUAGE language) {
    this.language = language;
    mEquivalenceClasses = ImmutableSet.copyOf(new ArrayList<ChunkEquivalenceClass>());
  }

  public ChunkQuotientSet(E2eConstants.LANGUAGE language, Collection<ChunkEquivalenceClass> equivalenceClasses) {
    this.language = language;
    mEquivalenceClasses = ImmutableSet.copyOf(equivalenceClasses);
  }

  public boolean areEquivalent(Chunk c1, Chunk c2) {

    //If we are comparing EntityMentions, and their mentionTypes are different, they only match if the offsets
    // are exactly the same
//        if(c1 instanceof EntityMention && c2 instanceof  EntityMention) {
//            if(!((EntityMention) c1).getMentionType().equals(((EntityMention) c2).getMentionType())) {
//                return exactMatch(c1, c2);
//            }
//        }

    //For chunks other than entitymentions we use the standard matching for chunks
    if (exactMatch(c1, c2)) {
      return true;
    } else if (noArticleMatch(c1, c2)) {
      return true;
    } else if(c1 instanceof EntityMention && c2 instanceof EntityMention ){
          if (((EntityMention) c1).getMentionType().getType().equalsIgnoreCase("LIST") ||
              ((EntityMention) c2).getMentionType().getType().equalsIgnoreCase("LIST")) {
            return false; //For LIST EntityMentions, if exact and no-article matches have failed, return false
          }
        //Handling appositives from Serif
        //Serif produces appositives with the NP it modifies, e.g. "US Senator from Massachusetts, Elizabeth Warren". This can result in this span not
        //matching with either "US Senator from Massachusetts" (span-end not matching) or "Elizabeth Warren" (presence of a preposition)
        //If either EntityMention is appositive and has a "head" chunk, use that for lastCharMatch check.
        //Serif uses the first child mention as head, so for the above example of Appositive mention, the head from Serif
        //would be "US Senator from Massachusetts". To accommodate for cases where "Elizabeth Warren" is the other mention to match, if the match with
        //Appositive's head fails, we will try to match with the rest of the span as well
        EntityMention appositiveMention=null;
        EntityMention otherMention=null;
        if(((EntityMention)c1).getMentionType().getType().toUpperCase().startsWith("APPO")){
          appositiveMention = (EntityMention)c1;
          otherMention = (EntityMention)c2;
        }else if(((EntityMention)c2).getMentionType().getType().toUpperCase().startsWith("APPO")){
          appositiveMention = (EntityMention)c2;
          otherMention = (EntityMention)c1;
        }
        Chunk appositiveHead = appositiveMention!=null&&appositiveMention.getHeadOffset().isPresent()?(new Chunk(appositiveMention.getHeadOffset().get(),appositiveMention.getTokenStream())):null;
        if(appositiveHead!=null){
          if(lastCharMatch(appositiveHead,otherMention)){
            return true;
          }
          int mentionBegin = appositiveMention.getTokenOffset().getBegin();
          int mentionEnd = appositiveMention.getTokenOffset().getEnd();
          int appositiveBegin = appositiveHead.getTokenOffset().getBegin();
          int appositiveEnd = appositiveHead.getTokenOffset().getEnd();
          int remainingSpanBegin = mentionBegin<appositiveBegin?mentionBegin:appositiveEnd+1;
          int remainingSpanEnd = mentionBegin<appositiveBegin?appositiveBegin:mentionEnd;
          if(remainingSpanBegin>=mentionEnd || remainingSpanEnd==mentionBegin){
            //the head is the same as appositive, there's no remaining span
          }else {
            Chunk remainingSpan = new Chunk(new TokenOffset(remainingSpanBegin,remainingSpanEnd),appositiveMention.getTokenStream());
            if(lastCharMatch(remainingSpan,otherMention)){
              return true;
            }
          }
        }
    }
    if(lastCharMatch(c1, c2)){
      return true;
    } else if (exactHeadMatch(c1,c2)){
        return true;
    }
//    else if(headContainmentMatch(c1,c2)){
//      return true;
//    }
//    else if(containmentMatch(c1,c2)){
//      return true;
//    }
    return false;
  }

  public boolean areEquivalentByContainmentInPivot(Chunk pivotChunk, Chunk nonPivotChunk){
    //use D alignment
    //if the nonPivotChunk is contained in the pivotChunk, consider it a match
    return contains(pivotChunk,nonPivotChunk);
  }

  // Optional because the chunk may be totally unaligned...
  public Optional<ChunkEquivalenceClass> equivalenceClassContaining(Chunk c) {
    for (ChunkEquivalenceClass eqClass : mEquivalenceClasses) {
      for (Chunk chunk : eqClass.chunks()) {
        if (chunk.equals(c)) {
          return Optional.of(eqClass);
        }
      }
    }
    return Optional.absent();
  }

  public Collection<ChunkEquivalenceClass> equivalenceClasses() {
    return mEquivalenceClasses;
  }

  public Iterator<ChunkEquivalenceClass> iterator() {
    throw new UnsupportedOperationException("Implement me");
  }

  private Boolean exactMatch(Chunk chunk1, Chunk chunk2) {
    int chunk1Begin = chunk1.getTokenStream().get(chunk1.getTokenOffset().getBegin())
        .getCharOffset().getBegin();

    int chunk1End = chunk1.getTokenStream().get(chunk1.getTokenOffset().getEnd())
        .getCharOffset().getEnd();

    int chunk2Begin = chunk2.getTokenStream().get(chunk2.getTokenOffset().getBegin())
        .getCharOffset().getBegin();

    int chunk2End = chunk2.getTokenStream().get(chunk2.getTokenOffset().getEnd())
        .getCharOffset().getEnd();

    boolean beginSame = chunk1Begin == chunk2Begin;
    boolean endSame = chunk1End == chunk2End;
    boolean valueSame = chunk1.getValue().equals(chunk2.getValue());

    return (beginSame && endSame && valueSame);
  }

  private Boolean noArticleMatch(Chunk chunk1, Chunk chunk2) {
    int chunk1End =
        chunk1.getTokenStream().get(chunk1.getTokenOffset().getEnd()).getCharOffset().getEnd();
    int chunk2End =
        chunk2.getTokenStream().get(chunk2.getTokenOffset().getEnd()).getCharOffset().getEnd();
    String determiners = determinersByLanguage.get(language);
        
    Chunk shorterChunk = chunk1.getValue().length() <= chunk2.getValue().length() ? chunk1 : chunk2;
    Chunk longerChunk = shorterChunk == chunk1 ? chunk2 : chunk1;
    Pattern p = Pattern
        .compile(determiners + Pattern.quote(shorterChunk.getValue()), Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(longerChunk.getValue());
    return (chunk1End == chunk2End && m.matches());
  }

  private Boolean lastCharMatch(Chunk chunk1, Chunk chunk2) {
    int chunk1End =
        chunk1.getTokenStream().get(chunk1.getTokenOffset().getEnd()).getCharOffset().getEnd();
    int chunk2End =
        chunk2.getTokenStream().get(chunk2.getTokenOffset().getEnd()).getCharOffset().getEnd();
    Chunk shorterChunk = chunk1.getValue().length() <= chunk2.getValue().length() ? chunk1 : chunk2;
    Chunk longerChunk = shorterChunk == chunk1 ? chunk2 : chunk1;
    String prepositions = prepositionsByLanguage.get(language);
    
    Pattern p =
        Pattern.compile(Pattern.quote(shorterChunk.getValue()) + "$", Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(longerChunk.getValue());
    if (m.find()) {
      p = Pattern.compile(prepositions, Pattern.CASE_INSENSITIVE);
      m = p.matcher(longerChunk.getValue());
      return (chunk1End == chunk2End && !m.find());
    } else {
      return false;
    }
  }

  private boolean exactHeadMatch(Chunk c1, Chunk c2){
    if(!getHead(c1).isPresent()&&!getHead(c2).isPresent()){
      return false;
    }
    return exactMatch(getHead(c1).isPresent()?getHead(c1).get():c1,
        getHead(c2).isPresent()?getHead(c2).get():c2);
  }

//  private boolean headContainmentMatch(Chunk c1, Chunk c2){
//    if(!getHead(c1).isPresent()||!getHead(c2).isPresent()){
//      return false;
//    }
//    Chunk head1 = getHead(c1).get():c1;
//    Chunk head2 = getHead(c2).isPresent()?getHead(c2).get():c2;
//    return contains(head1,head2)||contains(head2,head1);
//  }

  private boolean contains(Chunk c1, Chunk c2){
    return (c2.getCharOffset().getBegin() >= c1.getCharOffset().getBegin())
        && (c2.getCharOffset().getEnd() <= c1.getCharOffset().getEnd());
  }

  private Optional<Chunk> getHead(Chunk c){
    Chunk retVal = null;
    if (c instanceof EntityMention && ((EntityMention) c).getHead().isPresent()) {
      retVal = new Chunk(((EntityMention) c).getHeadOffset().get(),
          ((EntityMention) c).getTokenStream());
    }
    return Optional.fromNullable(retVal);
  }

}
