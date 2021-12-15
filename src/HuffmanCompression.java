import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

class ByteArrayWrapper implements Serializable, Comparable<ByteArrayWrapper> {
    public final byte[] data;

    public ByteArrayWrapper(byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ByteArrayWrapper))
            return false;
        return Arrays.equals(data, ((ByteArrayWrapper)other).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for(byte x: data)
            s.append((char) x).append(" ");
        return s.toString();
    }

    @Override
    public int compareTo(ByteArrayWrapper byteArrayWrapper) {
        for (int i = 0; i < data.length;i++)
            if(data[i] != byteArrayWrapper.data[i])
                return data[i] - byteArrayWrapper.data[i];
        return 0;
    }
}


class TreeNode implements Serializable {
    public int left;
    public int right;

    TreeNode() {
        left = -1;
        right = -1;
    }
}

class LeafNode extends TreeNode {
    public byte[] character;

    LeafNode(byte[] character) {
        this.character = character;
    }
}


class MetaData {
    Map<ByteArrayWrapper, Integer> characterCount;
    int numberOfLeafs;
    int numberOfBits;
    byte[] lastPart;
    int groupSize;

    public MetaData() {
        characterCount = new TreeMap<>();
        lastPart = null;
    }


    public void writeMetaData(OutputStream outputStream) throws IOException {
        outputStream.write(ByteBuffer.allocate(4).putInt(numberOfBits).array());
        outputStream.write(ByteBuffer.allocate(4).putInt(groupSize).array());
        outputStream.write(ByteBuffer.allocate(4).putInt(numberOfLeafs).array());

        for(Map.Entry<ByteArrayWrapper, Integer> entry: characterCount.entrySet()) {
            outputStream.write(entry.getKey().data);
            outputStream.write(ByteBuffer.allocate(4).putInt(entry.getValue()).array());
        }

        if(lastPart == null)
            outputStream.write(ByteBuffer.allocate(4).putInt(0).array());
        else {
            outputStream.write(ByteBuffer.allocate(4).putInt(lastPart.length).array());
            outputStream.write(lastPart);
        }
    }

    public static MetaData readMetaData(InputStream inputStream) throws IOException {
        MetaData metaData = new MetaData();

        byte[] integerBuffer = new byte[4];

        inputStream.read(integerBuffer);
        metaData.numberOfBits = ByteBuffer.wrap(integerBuffer).getInt();

        inputStream.read(integerBuffer);
        metaData.groupSize = ByteBuffer.wrap(integerBuffer).getInt();

        inputStream.read(integerBuffer);
        metaData.numberOfLeafs = ByteBuffer.wrap(integerBuffer).getInt();

        for(int i = 0; i < metaData.numberOfLeafs;i++) {
            byte[] character = new byte[metaData.groupSize];
            inputStream.read(character);

            inputStream.read(integerBuffer);
            int freq = ByteBuffer.wrap(integerBuffer).getInt();
            metaData.characterCount.put(new ByteArrayWrapper(character), freq);
        }


        inputStream.read(integerBuffer);
        int lastPartSize = ByteBuffer.wrap(integerBuffer).getInt();
        if(lastPartSize > 0) {
            metaData.lastPart = new byte[lastPartSize];
            inputStream.read(metaData.lastPart);
        }

        return metaData;
    }
}

class NodeWrapper implements Comparable<NodeWrapper> {
    int nodeIndex;
    int count;

    public NodeWrapper(int nodeIndex, int count) {
        this.nodeIndex = nodeIndex;
        this.count = count;
    }

    @Override
    public int compareTo(NodeWrapper nodeWrapper) {
        return count - nodeWrapper.count;
    }
}

public class HuffmanCompression {


    Map<ByteArrayWrapper, byte[]> characterToCompressed;
    List<TreeNode> nodeList;
    private static final int BUFFER_SIZE = 8192;


    public HuffmanCompression() {
        characterToCompressed = new HashMap<>();
        nodeList = new ArrayList<>();
//        nodeList = new ArrayList<>();
//        numberOfBits = 0;
    }

    public void readCharacterCounts(MetaData metaData, String path, int groupSize) {
//        Map<ByteArrayWrapper, Integer> characterCount = new TreeMap<>();
        try(InputStream ios = new FileInputStream(path)) {
            byte[] bufferRead = new byte[groupSize * BUFFER_SIZE];
            byte[] buffer = new byte[groupSize];
            int readBytes;
            while ((readBytes = ios.read(bufferRead)) != -1) {
                for (int groupIndex = 0; groupIndex + groupSize <= readBytes; groupIndex += groupSize) {
                    System.arraycopy(bufferRead, groupIndex, buffer, 0, groupSize);
                    int currentCount = metaData.characterCount.getOrDefault(new ByteArrayWrapper(buffer), 0);
                    metaData.characterCount.put(new ByteArrayWrapper(buffer), currentCount + 1);
                    buffer = new byte[groupSize];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generateHuffmanTree(MetaData metaData) {
        PriorityQueue<NodeWrapper> queue = new PriorityQueue<>();

        for(Map.Entry<ByteArrayWrapper, Integer> entry: metaData.characterCount.entrySet()) {
            LeafNode leafNode = new LeafNode(entry.getKey().data);
            NodeWrapper wrapper = new NodeWrapper(nodeList.size(), entry.getValue());
            queue.add(wrapper);

            nodeList.add(leafNode);
        }

        metaData.numberOfLeafs = nodeList.size();

        while (queue.size() > 1) {
            NodeWrapper leftWrapper = queue.poll();
            NodeWrapper rightWrapper = queue.poll();

            TreeNode newNode = new TreeNode();
            NodeWrapper newNodeWrapper = new NodeWrapper(nodeList.size(), leftWrapper.count + rightWrapper.count);
            newNode.left = leftWrapper.nodeIndex;
            newNode.right = rightWrapper.nodeIndex;

            queue.add(newNodeWrapper);
            nodeList.add(newNode);
        }
    }


    private void generateCodeWords(int rootIndex, List<Byte> compressed) {
        if(rootIndex == -1)
            return;
        TreeNode root = nodeList.get(rootIndex);
        if(root.getClass() == LeafNode.class) {
            byte[] compressedRepArray = new byte[compressed.size()];
            for(int i = 0; i < compressedRepArray.length; i++)
                compressedRepArray[i] = compressed.get(i);
            byte[] characterByteArray = ((LeafNode) root).character;
            characterToCompressed.put(new ByteArrayWrapper(characterByteArray), compressedRepArray);
            return;
        }
        compressed.add((byte) 0);
        generateCodeWords(root.left, compressed);
        compressed.remove(compressed.size() - 1);
        compressed.add((byte) 1);
        generateCodeWords(root.right, compressed);
        compressed.remove(compressed.size() - 1);
    }

    private void generateCodeWords() {
        List<Byte> compressedRep = new ArrayList<>();
//        TreeNode root = nodeList.get(nodeList.size() - 1);
        generateCodeWords(nodeList.size() - 1, compressedRep);
    }


    private void compress(String path, int groupSize) throws IOException {
        Instant startTime = Instant.now();
        MetaData metaData = new MetaData();
        metaData.groupSize = groupSize;
        readCharacterCounts(metaData, path, groupSize);
        generateHuffmanTree(metaData);
        generateCodeWords();

        try(InputStream ios = new FileInputStream(path);
            OutputStream outputStream = new FileOutputStream(path + ".tmp")) {
            byte[] buffer = new byte[groupSize];
            byte[] readBuffer = new byte[groupSize * BUFFER_SIZE];
            int readBytes;
            int bitIndex = 0;
            byte byteToWrite = 0;

            byte[] writeBuffer = new byte[BUFFER_SIZE];
            int writeBufferIndex = 0;
            while ((readBytes = ios.read(readBuffer)) != -1) {
                int groupIndex = 0;
                for(; groupIndex + groupSize <= readBytes; groupIndex += groupSize) {
                    System.arraycopy(readBuffer, groupIndex, buffer, 0, groupSize);
                    byte[] compressedBits = characterToCompressed.get(new ByteArrayWrapper(buffer));

                    for(byte bit: compressedBits) {
                        metaData.numberOfBits++;
                        if(bit == 1)
                            byteToWrite |= (1 << bitIndex);
                        bitIndex++;
                        if (bitIndex == 8) {
                            writeBuffer[writeBufferIndex++] = byteToWrite;
                            if(writeBufferIndex == BUFFER_SIZE) {
                                writeBufferIndex = 0;
                                outputStream.write(writeBuffer);
                            }
                            byteToWrite = 0;
                            bitIndex = 0;
                        }
                    }
                }
                if (groupIndex != readBytes) {
                    int len = readBytes - groupIndex;
                    metaData.lastPart = new byte[len];
                    System.arraycopy(readBuffer, groupIndex, metaData.lastPart, 0, len);
                }
            }
            if(bitIndex > 0)
                writeBuffer[writeBufferIndex++] = byteToWrite;

            if(writeBufferIndex > 0)
                outputStream.write(writeBuffer, 0, writeBufferIndex);

        } catch (Exception e) {
            e.printStackTrace();
        }

//        MetaData metaData = new MetaData(nodeList, numberOfBits, lastPart, groupSize, numberOfLeafs, nodeList.size());
//        byte[] metaDataBytes = null;
//        try(
//                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
//            objectOutputStream.writeObject(metaData);
//            objectOutputStream.flush();
//            metaDataBytes = byteArrayOutputStream.toByteArray();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        File file = new File(path);
        String fileName = file.getName();
        String compressedFileName = "18010078." + fileName + ".hc";
        File compressedFile = new File(file.getParentFile(), compressedFileName);

        try(OutputStream outputStream = new FileOutputStream(compressedFile);
            InputStream inputStream = new FileInputStream(path + ".tmp")) {

//            outputStream.write(ByteBuffer.allocate(4).putInt(metaDataBytes.length).array());
//            outputStream.write(metaDataBytes);
            metaData.writeMetaData(outputStream);
            byte[] buffer = new byte[BUFFER_SIZE];
            int readBytes;
            while ((readBytes = inputStream.read(buffer)) != -1)
                outputStream.write(buffer, 0, readBytes);
        }
        File tmpFile = new File(path + ".tmp");
        tmpFile.delete();
        Instant endTime = Instant.now();
        Duration duration = Duration.between(startTime, endTime);
        System.out.println("Execution Time: " +
                LocalTime.MIDNIGHT.plus(duration).format(DateTimeFormatter.ofPattern("HH:mm:ss SSS")));
        long sizeBeforeCompression = Files.size(file.toPath());
        long sizeAfterCompression = Files.size(compressedFile.toPath());
        double compressionRatio = (1 - (double) sizeAfterCompression / sizeBeforeCompression) * 100.0;
        System.out.println("Size Before Compression: " + String.format("%,d", sizeBeforeCompression) + " bytes");
        System.out.println("Size After Compression: " + String.format("%,d", sizeAfterCompression) + " bytes");
        System.out.println();
        System.out.println("Compression Ratio: " + String.format("%.2f", compressionRatio) + "%");
    }

    public void decompress(String path) {
        Instant startTime = Instant.now();
        try (InputStream fileStream = new FileInputStream(path)) {
//            byte[] sizeBuffer = new byte[4];
//            fileStream.read(sizeBuffer);
//            int size = ByteBuffer.wrap(sizeBuffer).getInt();
//            byte[] metaDataBytes = new byte[size];
//            fileStream.read(metaDataBytes);
//            MetaData metaData;
//            try(ObjectInput objectInput = new ObjectInputStream(new ByteArrayInputStream(metaDataBytes))) {
//                metaData = (MetaData) objectInput.readObject();
//            }

            MetaData metaData = MetaData.readMetaData(fileStream);
            generateHuffmanTree(metaData);
            generateCodeWords();
            File compressedFile = new File(path);
            String fileName = compressedFile.getName();
            String decompressedFileName = "extracted." + fileName.substring(0, fileName.length() - 3);
            String decompressedFilePath = new File(compressedFile.getParentFile(), decompressedFileName).toString();

            try (OutputStream outputStream = new FileOutputStream(decompressedFilePath)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                byte[] writeBuffer = new byte[BUFFER_SIZE * metaData.groupSize];
                int writeIndex = 0;
                int readBytes;
                TreeNode root = nodeList.get(nodeList.size() - 1);
                TreeNode cur = root;

                while ((readBytes = fileStream.read(buffer)) != -1) {
                    for(int i = 0; i < metaData.numberOfBits;i++) {
                        int bufferIndex = i / 8;
                        if(bufferIndex >= readBytes)
                            break;
                        int bitIndex = i % 8;
                        int bit = buffer[bufferIndex] & (1 << bitIndex);
                        cur = nodeList.get(bit == 0? cur.left : cur.right);

                        if(cur.getClass() == LeafNode.class) {
                            System.arraycopy(((LeafNode) cur).character, 0, writeBuffer, writeIndex, metaData.groupSize);
                            writeIndex += metaData.groupSize;
                            if(writeIndex == writeBuffer.length) {
                                outputStream.write(writeBuffer);
                                writeIndex = 0;
                            }
                            cur = root;
                        }
                    }
                    metaData.numberOfBits -= readBytes * 8;
                }
                if(writeIndex > 0)
                    outputStream.write(writeBuffer, 0, writeIndex);

                if(metaData.lastPart != null)
                    outputStream.write(metaData.lastPart);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Instant endTime = Instant.now();
        Duration duration = Duration.between(startTime, endTime);
        System.out.println("Execution Time: " +
                LocalTime.MIDNIGHT.plus(duration).format(DateTimeFormatter.ofPattern("HH:mm:ss SSS")));
    }

    public static void main(String[] args) throws IOException {
        if(args.length == 0)
            System.out.println("invalid arguments");
        else if(args[0].equals("c")) {
            if(args.length != 3)
                System.out.println("invalid arguments");
            else {
                HuffmanCompression huffmanCompression = new HuffmanCompression();
                huffmanCompression.compress(args[1], Integer.parseInt(args[2]));
            }
        }
        else if (args[0].equals("d")) {
            if(args.length != 2)
                System.out.println("invalid arguments");
            else {
                HuffmanCompression huffmanCompression = new HuffmanCompression();
                huffmanCompression.decompress(args[1]);
            }
        }
    }
}
