/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.jts.index.quadtree;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.serde.shape.ShapeGeometrySerde;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides methods to efficiently serialize and deserialize the index.
 * trees are serialized recursively.
 */
public class IndexSerde
{
    ShapeGeometrySerde geometrySerde;
    public IndexSerde() {
        geometrySerde = new ShapeGeometrySerde();
    }

    public Object read(Kryo kryo, Input input){
        Quadtree index = new Quadtree();
        boolean notEmpty = (input.readByte() & 0x01) == 1;
        if (!notEmpty) { return index; }
        int itemSize = input.readInt();
        List items = new ArrayList();
        for (int i = 0; i < itemSize; ++i) {
            items.add(geometrySerde.read(kryo, input, Geometry.class));
        }
        index.getRoot().items = items;
        for (int i = 0; i < 4; ++i) {
            index.getRoot().subnode[i] = readQuadTreeNode(kryo, input);
        }
        return index;
    }

    public void write(Kryo kryo, Output output, Quadtree tree) {
        // serialize quadtree index
        if (tree.isEmpty()) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            // write root
            List items = tree.getRoot().getItems();
            output.writeInt(items.size());
            for (Object item : items) {
                geometrySerde.write(kryo, output, item);
            }
            Node[] subNodes = tree.getRoot().subnode;
            for (int i = 0; i < 4; ++i) {
                writeQuadTreeNode(kryo, output, subNodes[i]);
            }
        }
    }

    private void writeQuadTreeNode(Kryo kryo, Output output, Node node)
    {
        // write head first
        if (node == null || node.isEmpty()) {
            output.writeByte(0);
        }
        else { // not empty
            output.writeByte(1);
            // write node information, envelope and level
            geometrySerde.write(kryo, output, node.getEnvelope());
            output.writeInt(node.getLevel());
            List items = node.getItems();
            output.writeInt(items.size());
            for (Object obj : items) {
                geometrySerde.write(kryo, output, obj);
            }
            Node[] subNodes = node.subnode;
            for (int i = 0; i < 4; ++i) {
                writeQuadTreeNode(kryo, output, subNodes[i]);
            }
        }
    }

    private Node readQuadTreeNode(Kryo kryo, Input input)
    {
        boolean notEmpty = (input.readByte() & 0x01) == 1;
        if (!notEmpty) { return null; }
        Envelope envelope = (Envelope) geometrySerde.read(kryo, input, Envelope.class);
        int level = input.readInt();
        Node node = new Node(envelope, level);
        int itemSize = input.readInt();
        List items = new ArrayList();
        for (int i = 0; i < itemSize; ++i) {
            items.add(geometrySerde.read(kryo, input, Geometry.class));
        }
        node.items = items;
        // read children
        for (int i = 0; i < 4; ++i) {
            node.subnode[i] = readQuadTreeNode(kryo, input);
        }
        return node;
    }
}
