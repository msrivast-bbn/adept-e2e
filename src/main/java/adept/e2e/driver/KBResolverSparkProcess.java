package adept.e2e.driver;

import adept.common.KBID;
import adept.kbapi.KBParameters;
import com.google.common.collect.ImmutableList;

import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

import adept.e2e.kbresolver.KBResolverAbstractModule;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by bmin on 11/30/16.
 */
public class KBResolverSparkProcess {
    Iterable<KBResolverAbstractModule> modules;

    public Iterable<KBResolverAbstractModule> getModules() {
        return modules;
    }

    public KBResolverSparkProcess(Iterable<KBResolverAbstractModule> modules) {
        this.modules = modules;
    }

    public void run() throws InvalidPropertiesFormatException {
        for(KBResolverAbstractModule kbResolverAbstractModule: modules) {
            kbResolverAbstractModule.run();
        }
    }

    public static class Builder {
        private ImmutableList.Builder<KBResolverAbstractModule> moduleListBuilder;

        public Builder() {
            moduleListBuilder = new ImmutableList.Builder<KBResolverAbstractModule>();
        }

        public Builder withModule(KBResolverAbstractModule kbResolverAbstractModule) {
            moduleListBuilder.add(kbResolverAbstractModule);
            return this;
        }

        public KBResolverSparkProcess build() {
            List<KBResolverAbstractModule> modules = moduleListBuilder.build();
            return new KBResolverSparkProcess(modules);
        }
    }

}
