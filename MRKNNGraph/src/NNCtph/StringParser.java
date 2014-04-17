package NNCtph;

import MRKNNGraph.Node;
import java.io.Serializable;

/**
 *
 * @author tibo
 */
public interface StringParser extends Serializable{
    public Node parse(String s);
}
