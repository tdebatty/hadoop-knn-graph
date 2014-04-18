package NNCtph;

import MRKNNGraph.Node;
import info.debatty.stringsimilarity.JaroWinkler;

/**
 *
 * @author tibo
 */
public class DefaultSimilarityCalculator implements SimilarityCalculator {
    @Override
    public double similarity(Node n1, Node n2) {
        return JaroWinkler.Similarity(n1.value, n2.value);
    }
}
