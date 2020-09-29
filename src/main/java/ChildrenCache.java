import java.util.ArrayList;
import java.util.List;

/**
 * Auxiliary cache to handle changes to the lists of tasks and of workers.
 */

public class ChildrenCache {

    protected List<String> children;

    ChildrenCache() {
        this.children = null;
    }

    ChildrenCache(List<String> children) {
        this.children = children;
    }

    List<String> getList() {
        return children;
    }

    List<String> addedAndSet(List<String> newChildren) {
        ArrayList<String> diff = null;

        if (children == null) {
            diff = new ArrayList<>(newChildren);
        } else {
            for (String s: newChildren) {
                if (!children.contains(s)) {
                    if (diff == null) {
                        diff = new ArrayList<>();
                    }

                    diff.add(s);
                }
            }
        }
        this.children = newChildren;

        return diff;
    }

    List<String> removedAndSet(List<String> newChildren) {
        List<String> diff = null;

        if (children != null) {
            for (String s: children) {
                if (!newChildren.contains(s)) {
                    if (diff == null) {
                        diff = new ArrayList<>();
                    }

                    diff.add(s);
                }
            }
        }
        this.children = newChildren;

        return diff;
    }
}