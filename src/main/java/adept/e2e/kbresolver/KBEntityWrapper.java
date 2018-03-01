package adept.e2e.kbresolver;

import adept.common.KBID;
import adept.common.OntType;
import adept.e2e.driver.SerializerUtil;
import adept.kbapi.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * KBEntity is not serializable
 * Neither is KBProvenance or KBTextProvenance
 * The wrapper keeps only the KBID and the canonical strings of each provenance
 * This is enough to get most of the way through SplitEntityMentionOutliers
 * Created by iheintz on 5/10/17.
 */
public class KBEntityWrapper implements Serializable {

    private static final long serialVersionUID = -63839808372079863L;
    private KBID kbid;
    private List<String> canonicalStrings;
    public KBEntityWrapper(KBEntity kbEntity) throws KBQueryException {
        kbid = kbEntity.getKBID();
        canonicalStrings = Lists.newArrayList();
        for (KBProvenance prov: kbEntity.getProvenances())
            canonicalStrings.add(prov.getValue());

    }

    public KBID getKbid() {
        return kbid;
    }

    public List<String> getCanonicalStrings() {
        return canonicalStrings;
    }

}
