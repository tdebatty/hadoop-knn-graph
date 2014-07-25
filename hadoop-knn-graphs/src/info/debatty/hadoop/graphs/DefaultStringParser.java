package info.debatty.hadoop.graphs;

import info.debatty.graphs.Node;

/**
 *
 * @author tibo
 */
public class DefaultStringParser implements StringParserInterface{

    @Override
    public Node parse(String s) {
        String[] pieces = s.split("\"", 4);
        Node n = new Node();
        n.id = pieces[1];
        n.value = pieces[3];
        return n;
    }
}
