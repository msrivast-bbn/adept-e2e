package adept.e2e.kbresolver.pruneLowConfRelations;

import adept.common.KBID;
import adept.e2e.driver.SerializerUtil;
import adept.kbapi.KBParameters;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by iheintz on 4/13/17.
 */
public class KBIDToProcessPair implements PairFunction<KBID, String, KBID> {
    private static KBParameters kbParameters;

    public KBIDToProcessPair(KBParameters kbParameters) {
        this.kbParameters = kbParameters;
    }

    @Override
    public Tuple2<String, KBID> call(KBID kbid) throws Exception {
        return new Tuple2<>(SerializerUtil.serialize(kbParameters), kbid);
    }
}
