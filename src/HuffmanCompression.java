import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

class ByteArrayWrapper implements Serializable {
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
        String s = "";
        for(byte x: data)
            s += (char)x + " ";
        return s;
    }
}


class TreeNode implements Comparable<TreeNode>, Serializable {
    public int count;
    public TreeNode left;
    public TreeNode right;

    TreeNode(int count) {
        this.count = count;
        left = null;
        right = null;
    }

    @Override
    public int compareTo(TreeNode treeNode) {
        return count - treeNode.count;
    }
}

class LeafNode extends TreeNode {
    public BitSet character;

    LeafNode(byte[] character, int count) {
        super(count);
        this.character = new BitSet(character.length);
        for(int i = 0; i < character.length;i++)
            if(character[i] == 1)
                this.character.set(i);
    }
}


class MetaData implements Serializable {
//    Map<ByteArrayWrapper, byte[]> compressedToCharacter;
    List<TreeNode> nodeList;
    int numberOfBits;
    int groupSize;
    byte[] lastPart;

    public MetaData(List<TreeNode> nodeList, int numberOfBits, byte[] lastPart) {
        this.nodeList = nodeList;
        this.numberOfBits = numberOfBits;
        this.lastPart = lastPart;
    }

    public void writeMetaData(OutputStream stream) {

    }
}


public class HuffmanCompression {


    Map<ByteArrayWrapper, byte[]> characterToCompressed;
    Map<ByteArrayWrapper, byte[]> compressedToCharacter;
    List<TreeNode> nodeList;
    int numberOfBits;
    byte[] lastPart;


    public HuffmanCompression() {
        characterToCompressed = new HashMap<>();
        compressedToCharacter = new HashMap<>();
        nodeList = new ArrayList<>();
        numberOfBits = 0;
    }

    public Map<ByteArrayWrapper, Integer> readCharacterCounts(String path, int groupSize) throws IOException {
        HashMap<ByteArrayWrapper, Integer> characterCount = new HashMap<>();
        // TODO: handle remaining bytes
        try(InputStream ios = new FileInputStream(path)) {
            byte[] buffer = new byte[groupSize];    // TODO: Bulk read
            int readBytes = 0;
            while ((readBytes = ios.read(buffer)) != -1 && readBytes == groupSize) {
                int currentCount = characterCount.getOrDefault(new ByteArrayWrapper(buffer), 0);
                characterCount.put(new ByteArrayWrapper(buffer), currentCount + 1);
                buffer = new byte[groupSize];
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return characterCount;
    }

    public void generateHuffmanTree(Map<ByteArrayWrapper, Integer> characterCounts) throws IOException {
        PriorityQueue<TreeNode> queue = new PriorityQueue<>();
        for(Map.Entry<ByteArrayWrapper, Integer> entry: characterCounts.entrySet()) {
            LeafNode leafNode = new LeafNode(entry.getKey().data, entry.getValue());
            queue.add(leafNode);
            nodeList.add(leafNode);
        }

        while (queue.size() > 1) {
            TreeNode left = queue.poll();
            TreeNode right = queue.poll();

            TreeNode newNode = new TreeNode(left.count + right.count);
            newNode.left = left;
            newNode.right = right;

            queue.add(newNode);
            nodeList.add(newNode);
        }
    }


    private void generateCodeWords(TreeNode root, List<Byte> compressedRepr) {
        if(root == null)
            return;
        if(root.getClass() == LeafNode.class) {
            // TODO: handle the case when the group size is equal to the file size
            byte[] compressedReprArray = new byte[compressedRepr.size()];
            for(int i = 0; i < compressedReprArray.length; i++)
                compressedReprArray[i] = compressedRepr.get(i);
            byte[] characterByteArray = new byte[((LeafNode) root).character.length()];
            for(int i = 0; i < characterByteArray.length; i++)
                if(((LeafNode) root).character.get(i))
                    characterByteArray[i] = 1;
            characterToCompressed.put(new ByteArrayWrapper(characterByteArray), compressedReprArray);
            compressedToCharacter.put(new ByteArrayWrapper(compressedReprArray), characterByteArray);
            return;
        }
        compressedRepr.add((byte) 0);
        generateCodeWords(root.left, compressedRepr);
        compressedRepr.remove(compressedRepr.size() - 1);
        compressedRepr.add((byte) 1);
        generateCodeWords(root.right, compressedRepr);
        compressedRepr.remove(compressedRepr.size() - 1);
    }

    private void generateCodeWords() {
        List<Byte> compressedRepr = new ArrayList<>();
        TreeNode root = nodeList.get(nodeList.size() - 1);
        generateCodeWords(root, compressedRepr);
    }


    private void compress(String path, int groupSize) throws IOException {
        Map<ByteArrayWrapper, Integer> characterCount = readCharacterCounts(path, groupSize);
        generateHuffmanTree(characterCount);
        generateCodeWords();

        try(InputStream ios = new FileInputStream(path);
            OutputStream outputStream = new FileOutputStream(path + "compressed")) {
            byte[] buffer = new byte[groupSize];
            int readBytes;
            int bitIndex = 0;
            byte byteToWrite = 0;

            byte[] writeBuffer = new byte[4096];
            int writeBufferIndex = 0;
            while ((readBytes = ios.read(buffer)) != -1) {
                if(readBytes == groupSize) {
                    byte[] compressedBits = characterToCompressed.get(new ByteArrayWrapper(buffer));

                    for(byte bit: compressedBits) {
                        numberOfBits++;
                        if(bit == 1)
                            byteToWrite |= (1 << bitIndex);
                        bitIndex = (bitIndex + 1) % 8;
                        if (bitIndex == 0) {
                            writeBuffer[writeBufferIndex++] = byteToWrite;
                            if(writeBufferIndex == 4096) {
                                writeBufferIndex = 0;
                                outputStream.write(writeBuffer);
                            }
                            byteToWrite = 0;
                        }
                    }
                }
                else {
                    // TODO: check size
                    lastPart = new byte[readBytes];
                    System.arraycopy(buffer, 0, lastPart, 0, readBytes);
                }
            }
            if(bitIndex > 0) {
                writeBuffer[writeBufferIndex++] = byteToWrite;
//                outputStream.write(byteToWrite);
            }

            if(writeBufferIndex > 0)
                outputStream.write(writeBuffer, 0, writeBufferIndex);

        } catch (Exception e) {
            e.printStackTrace();
        }

        MetaData metaData = new MetaData(nodeList, numberOfBits, lastPart);
        byte[] metaDataBytes = null;
        try(
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(metaData);
            objectOutputStream.flush();
            metaDataBytes = byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try(OutputStream outputStream = new FileOutputStream(path + "huffman");
            InputStream inputStream = new FileInputStream(path + "compressed")) {

            outputStream.write(ByteBuffer.allocate(4).putInt(metaDataBytes.length).array());
            System.out.println("Metadata length: " + metaDataBytes.length);
            outputStream.write(metaDataBytes);
            byte[] buffer = new byte[4096];
            int readBytes;
            while ((readBytes = inputStream.read(buffer)) != -1)
                outputStream.write(buffer, 0, readBytes);
        }
    }

    public void decompress(String path) {
        try (InputStream fileStream = new FileInputStream(path)) {
            byte[] sizeBuffer = new byte[4];
            fileStream.read(sizeBuffer);
            int size = ByteBuffer.wrap(sizeBuffer).getInt();
            byte[] metaDataBytes = new byte[size];
            fileStream.read(metaDataBytes);
            MetaData metaData;
            try(ObjectInput objectInput = new ObjectInputStream(new ByteArrayInputStream(metaDataBytes))) {
                metaData = (MetaData) objectInput.readObject();
            }

            try (OutputStream outputStream = new FileOutputStream(path + "decompressed")) {
                byte[] buffer = new byte[4096];
                int readBytes;
                TreeNode root = metaData.nodeList.get(metaData.nodeList.size() - 1);
                TreeNode cur = root;

                while ((readBytes = fileStream.read(buffer)) != -1) {
                    for(int i = 0; i < metaData.numberOfBits;i++) {
                        int bufferIndex = i / 8;
                        if(bufferIndex >= readBytes)
                            break;
                        int bitIndex = i % 8;
                        int bit = buffer[bufferIndex] & (1 << bitIndex);
                        cur = bit == 0? cur.left : cur.right;

                        if(cur.getClass() == LeafNode.class) {
                            byte[] characterByteArray = new byte[((LeafNode) root).character.length()];
                            for(int j = 0; j < characterByteArray.length; j++)
                                if(((LeafNode) root).character.get(j))
                                    characterByteArray[j] = 1;
                            outputStream.write(characterByteArray);
                            cur = root;
                        }
                    }
                    metaData.numberOfBits -= readBytes * 8;
                }
                if(metaData.lastPart != null)
                    outputStream.write(metaData.lastPart);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String path = "/home/abe-mark45/Projects/AlgorithmsLab2/gbbct10.seq.gz";
        HuffmanCompression huffmanCompression = new HuffmanCompression();
        huffmanCompression.compress(path, 32);
        System.out.println("copress done");
        huffmanCompression.decompress("/home/abe-mark45/Projects/AlgorithmsLab2/gbbct10.seq.gzhuffman");

    }
}
