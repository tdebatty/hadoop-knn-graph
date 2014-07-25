package info.debatty.hadoop.graphs;

import info.debatty.graphs.SimilarityInterface;
import info.debatty.stringsimilarity.JaroWinkler;

/**
 *
 * @author tibo
 */
public class DefaultSimilarity implements SimilarityInterface {
    
    @Override
    public double similarity(info.debatty.graphs.Node n1, info.debatty.graphs.Node n2) {
        return JaroWinkler.Similarity((String) n1.value, (String) n2.value);
    }
}
