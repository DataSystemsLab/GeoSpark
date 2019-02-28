/*
 * FILE: ImageWrapperSerializer
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geosparkviz.core.Serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.datasyslab.geosparkviz.core.ImageSerializableWrapper;

import javax.imageio.ImageIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ImageWrapperSerializer
        extends Serializer<ImageSerializableWrapper>
{
    final static Logger log = Logger.getLogger(ImageWrapperSerializer.class);

    @Override
    public void write(Kryo kryo, Output output, ImageSerializableWrapper object)
    {
        try {
            log.debug("Serializing ImageSerializableWrapper...");
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ImageIO.write(object.getImage(), "png", byteArrayOutputStream);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] writeImage(ImageSerializableWrapper object)
    {
        Kryo kryo = new Kryo();
        Output output = new Output();
        write(kryo, output, object);
        return output.toBytes();
    }

    @Override
    public ImageSerializableWrapper read(Kryo kryo, Input input, Class<ImageSerializableWrapper> type)
    {
        try {
            log.debug("De-serializing ImageSerializableWrapper...");
            return new ImageSerializableWrapper(ImageIO.read(new ByteArrayInputStream(input.getBuffer())));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ImageSerializableWrapper readImage(byte[] inputArray)
    {
        Kryo kryo = new Kryo();
        Input input = new Input(inputArray);
        return read(kryo, input, ImageSerializableWrapper.class);
    }
}
