package fastColoringKryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;

public class RoaringSerializer2 extends Serializer<MutableRoaringBitmap> {
    @Override
    public void write(Kryo kryo, Output output, MutableRoaringBitmap bitmap) {
        System.out.println("KRYO TABARNAK");
        try {
            bitmap.serialize(new KryoDataOutput(output));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    @Override
    public MutableRoaringBitmap read(Kryo kryo, Input input, Class<MutableRoaringBitmap> type) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        try {
            bitmap.deserialize(new KryoDataInput(input));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        return bitmap;
    }
}
