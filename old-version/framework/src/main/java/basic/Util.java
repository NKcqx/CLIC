package basic;

import basic.operators.Operator;
import channel.Channel;
import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.swing.mxGraphComponent;
import org.jgrapht.ListenableGraph;
import org.jgrapht.ext.JGraphXAdapter;

import javax.swing.*;
import java.awt.*;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/14 9:59 上午
 */
public class Util {
    private static Supplier<String> idSupplier = () -> String.valueOf(UUID.randomUUID().hashCode());

    public static String generateID() {
        return Util.idSupplier.get();
    }


    public static void visualize(ListenableGraph<Operator, Channel> graph) {
        Visualizer applet = new Visualizer(graph);
        applet.init();

        JFrame frame = new JFrame();
        frame.getContentPane().add(applet);
        frame.setTitle("JGraphT Adapter to JGraphX Demo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    private static final class Visualizer extends JApplet {
        private static final long serialVersionUID = UUID.randomUUID().hashCode();
        private static final Dimension DEFAULT_SIZE = new Dimension(530, 320);
        private JGraphXAdapter<Operator, Channel> jgxAdapter;
        private ListenableGraph<Operator, Channel> graph;

        private Visualizer(ListenableGraph<Operator, Channel> graph) {
            this.graph = graph;
        }

        @Override
        public void init() {
            jgxAdapter = new JGraphXAdapter<>(graph);

            setPreferredSize(DEFAULT_SIZE);
            mxGraphComponent component = new mxGraphComponent(jgxAdapter);
            component.setConnectable(false);
            component.getGraph().setAllowDanglingEdges(false);
            getContentPane().add(component);
            resize(DEFAULT_SIZE);

            // positioning via jgraphx layouts
            mxCircleLayout layout = new mxCircleLayout(jgxAdapter);

            int radius = 100;
            layout.setX0((DEFAULT_SIZE.width / 2.0) - radius);
            layout.setY0((DEFAULT_SIZE.height / 2.0) - radius);
            layout.setRadius(radius);
            layout.setMoveCircle(true);

            layout.execute(jgxAdapter.getDefaultParent());
        }

    }

}
