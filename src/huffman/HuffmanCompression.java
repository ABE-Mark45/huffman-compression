package huffman;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
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
        StringBuilder s = new StringBuilder();
        for(byte x: data)
            s.append((char) x).append(" ");
        return s.toString();
    }
}


class TreeNode implements Serializable {
    public TreeNode left;
    public TreeNode right;

    TreeNode() {
        left = null;
        right = null;
    }
}

class LeafNode extends TreeNode {
    public byte[] character;

    LeafNode(byte[] character) {
        this.character = character;
    }
}


class MetaData implements Serializable {
    List<TreeNode> nodeList;
    long numberOfBits;
    byte[] lastPart;
    int groupSize;

    public MetaData(List<TreeNode> nodeList, long numberOfBits, byte[] lastPart, int groupSize) {
        this.nodeList = nodeList;
        this.numberOfBits = numberOfBits;
        this.lastPart = lastPart;
        this.groupSize = groupSize;
    }
}

class NodeWrapper implements Comparable<NodeWrapper> {
    TreeNode root;
    int count;

    public NodeWrapper(TreeNode root, int count) {
        this.root = root;
        this.count = count;
    }

    @Override
    public int compareTo(NodeWrapper nodeWrapper) {
        return count - nodeWrapper.count;
    }
}

public class HuffmanCompression {
    private static final int BUFFER_SIZE = 8192;

    private static Map<ByteArrayWrapper, Integer> readCharacterCounts(String path, int groupSize) {
        HashMap<ByteArrayWrapper, Integer> characterCount = new HashMap<>();
        try(InputStream ios = new FileInputStream(path)) {
            byte[] bufferRead = new byte[groupSize * BUFFER_SIZE];
            byte[] buffer = new byte[groupSize];
            int readBytes;
            while ((readBytes = ios.read(bufferRead)) != -1) {
                for (int groupIndex = 0; groupIndex + groupSize <= readBytes; groupIndex += groupSize) {
                    System.arraycopy(bufferRead, groupIndex, buffer, 0, groupSize);
                    int currentCount = characterCount.getOrDefault(new ByteArrayWrapper(buffer), 0);
                    characterCount.put(new ByteArrayWrapper(buffer), currentCount + 1);
                    buffer = new byte[groupSize];
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return characterCount;
    }

    public static void generateHuffmanTree(Map<ByteArrayWrapper, Integer> characterCounts, List<TreeNode> nodeList) {
        PriorityQueue<NodeWrapper> queue = new PriorityQueue<>();

        for(Map.Entry<ByteArrayWrapper, Integer> entry: characterCounts.entrySet()) {
            LeafNode leafNode = new LeafNode(entry.getKey().data);
            NodeWrapper wrapper = new NodeWrapper(leafNode, entry.getValue());
            queue.add(wrapper);

            nodeList.add(leafNode);
        }

        while (queue.size() > 1) {
            NodeWrapper leftWrapper = queue.poll();
            NodeWrapper rightWrapper = queue.poll();

            TreeNode newNode = new TreeNode();
            NodeWrapper newNodeWrapper = new NodeWrapper(newNode, leftWrapper.count + rightWrapper.count);
            newNode.left = leftWrapper.root;
            newNode.right = rightWrapper.root;

            queue.add(newNodeWrapper);
            nodeList.add(newNode);
        }
    }


    private static void generateCodeWords(TreeNode root, List<Boolean> compressed, Map<ByteArrayWrapper, boolean[]> characterToCompressed) {
        if(root == null)
            return;
        if(root.getClass() == LeafNode.class) {
            boolean[] compressedRepArray = new boolean[compressed.size()];
            for(int i = 0; i < compressedRepArray.length; i++)
                compressedRepArray[i] = compressed.get(i);
            byte[] characterByteArray = ((LeafNode) root).character;
            characterToCompressed.put(new ByteArrayWrapper(characterByteArray), compressedRepArray);
            return;
        }
        compressed.add(false);
        generateCodeWords(root.left, compressed, characterToCompressed);
        compressed.remove(compressed.size() - 1);
        compressed.add(true);
        generateCodeWords(root.right, compressed, characterToCompressed);
        compressed.remove(compressed.size() - 1);
    }

    private static void generateCodeWords(List<TreeNode> nodeList, Map<ByteArrayWrapper, boolean[]> characterToCompressed) {
        List<Boolean> compressedRep = new ArrayList<>();
        TreeNode root = nodeList.get(nodeList.size() - 1);
        generateCodeWords(root, compressedRep, characterToCompressed);
    }


    private static void compress(String path, int groupSize) throws IOException {
        Instant startTime = Instant.now();
        List<TreeNode> nodeList = new ArrayList<>();
        Map<ByteArrayWrapper, Integer> characterCount = readCharacterCounts(path, groupSize);
        Map<ByteArrayWrapper, boolean[]> characterToCompressed = new HashMap<>();
        generateHuffmanTree(characterCount, nodeList);
        generateCodeWords(nodeList, characterToCompressed);
        long numberOfBits = 0;
        byte[] lastPart = null;

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
                    boolean[] compressedBits = characterToCompressed.get(new ByteArrayWrapper(buffer));

                    for(boolean bit: compressedBits) {
                        numberOfBits++;
                        if(bit)
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
                    lastPart = new byte[len];
                    System.arraycopy(readBuffer, groupIndex, lastPart, 0, len);
                }
            }
            if(bitIndex > 0)
                writeBuffer[writeBufferIndex++] = byteToWrite;

            if(writeBufferIndex > 0)
                outputStream.write(writeBuffer, 0, writeBufferIndex);

        } catch (Exception e) {
            e.printStackTrace();
        }

        MetaData metaData = new MetaData(nodeList, numberOfBits, lastPart, groupSize);
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

        File file = new File(path);
        String fileName = file.getName();
        String compressedFileName = "18010078." + groupSize +"." + fileName + ".hc";
        File compressedFile = new File(file.getParentFile(), compressedFileName);

        try(OutputStream outputStream = new FileOutputStream(compressedFile);
            InputStream inputStream = new FileInputStream(path + ".tmp")) {

            outputStream.write(ByteBuffer.allocate(4).putInt(metaDataBytes.length).array());
            outputStream.write(metaDataBytes);
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

    public static void decompress(String path) {
        Instant startTime = Instant.now();
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
            File compressedFile = new File(path);
            String fileName = compressedFile.getName();
            String decompressedFileName = "extracted." + fileName.substring(0, fileName.length() - 3);
            String decompressedFilePath = new File(compressedFile.getParentFile(), decompressedFileName).toString();

            try (OutputStream outputStream = new FileOutputStream(decompressedFilePath)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                byte[] writeBuffer = new byte[BUFFER_SIZE * metaData.groupSize];
                int writeIndex = 0;
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
                            System.arraycopy(((LeafNode) cur).character, 0, writeBuffer, writeIndex, metaData.groupSize);
                            writeIndex += metaData.groupSize;
                            if(writeIndex == writeBuffer.length) {
                                outputStream.write(writeBuffer);
                                writeIndex = 0;
                            }
                            cur = root;
                        }
                    }
                    metaData.numberOfBits -= readBytes * 8L;
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
            else
                HuffmanCompression.compress(args[1], Integer.parseInt(args[2]));
        }
        else if (args[0].equals("d")) {
            if(args.length != 2)
                System.out.println("invalid arguments");
            else
                HuffmanCompression.decompress(args[1]);
        }
    }
}
