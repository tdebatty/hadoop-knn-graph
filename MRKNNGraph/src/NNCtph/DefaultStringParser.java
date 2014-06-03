package NNCtph;

import MRKNNGraph.Node;

/**
 *
 * @author tibo
 */
public class DefaultStringParser implements StringParser{

    @Override
    public Node parse(String s) {
        String[] pieces = s.split("\"", 4);
        Node n = new Node();
        n.id.set(pieces[1]);
        n.value = pieces[3];
        return n;

    }
}
