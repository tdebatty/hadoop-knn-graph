package info.debatty.hadoop.graphs;

import info.debatty.graphs.Node;
import java.io.Serializable;

/**
 *
 * @author tibo
 */
public interface StringParserInterface extends Serializable{
    public Node parse(String s);
}
