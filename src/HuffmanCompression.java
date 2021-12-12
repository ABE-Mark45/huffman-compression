import java.io.*;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class HuffmanCompression {
    private static class TreeNode implements Comparable<TreeNode> {
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

    private static class LeafNode extends TreeNode {
        public byte[] character;

        LeafNode(byte[] character, int count) {
            super(count);
            this.character = character;
        }
    }

    public static Map readCharacterCounts(String path, int groupSize) throws IOException {
        File inputFile = new File(path);
        byte[] fileContent = Files.readAllBytes(inputFile.toPath());
        HashMap<byte[], Integer> characterCount = new HashMap<>();
        // TODO: handle remaining bytes
        try(InputStream ios = new FileInputStream(path)) {
            byte[] buffer = new byte[groupSize];
            int readBytes = 0;

            while ((readBytes = ios.read(buffer)) != -1) {
                int currentCount = characterCount.getOrDefault(buffer, 0);
                characterCount.put(buffer, currentCount + 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return characterCount;
    }

    public static void compress(String path, int groupSize) throws IOException {
        Map<byte[], Integer> characterCounts = readCharacterCounts(path, groupSize);
        PriorityQueue<TreeNode> queue = new PriorityQueue<>();

        for(Map.Entry<byte[], Integer> entry: characterCounts.entrySet())
            queue.add(new LeafNode(entry.getKey(), entry.getValue()));

        while (queue.size() > 1) {
            TreeNode left = queue.poll();
            TreeNode right = queue.poll();

            TreeNode newNode = new TreeNode(left.count + right.count);
            newNode.left = left;
            newNode.right = right;

            queue.add(newNode);
        }

    }

    public static void main(String[] args) throws IOException {
        String path = "/home/abe-mark45/Projects/AlgorithmsLab2/input_test";

        readCharacterCounts(path, 4);

    }
}
