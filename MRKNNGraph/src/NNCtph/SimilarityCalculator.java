package NNCtph;

import MRKNNGraph.Node;
import java.io.Serializable;

/**
 *
 * @author tibo
 */
public interface SimilarityCalculator extends Serializable {
    public double similarity(Node n1, Node n2);
}
