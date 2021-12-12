import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class ActivitySelection {
    private static class Activity implements Comparable<Activity> {
        public int l;
        public int r;
        public int p;

        Activity(int l, int r, int p) {
            this.l = l;
            this.r = r;
            this.p = p;
        }

        @Override
        public int compareTo(Activity time) {
            if(r != time.r)
                return r - time.r;
            return l - time.l;
        }
    }

    private static int upperBound(int[] arr, int target) {
        int l = 0, r = arr.length - 1;

        while (l < r) {
            int m = (l + r) / 2;

            if(arr[m] <= target)
                l = m + 1;
            else
                r = m;
        }
        if(r == arr.length - 1 && target >= arr[r])
            return arr.length;
        return r;
    }

    public static int selectActivities(List<Activity> activityList) {
        int n = activityList.size();
        activityList.sort(Activity::compareTo);

        int[] memoTable = new int[n+1];
        Arrays.fill(memoTable, Integer.MIN_VALUE);
        int[] endIndex = new int[n+1];
        Arrays.fill(endIndex, Integer.MAX_VALUE);

        memoTable[0] = 0;
        endIndex[0] = 0;

        for(int i = 1; i <= n;i++) {
            Activity cur = activityList.get(i - 1);
            memoTable[i] = memoTable[i - 1];
            endIndex[i] = endIndex[i - 1];

            int j = upperBound(endIndex, cur.l) - 1;

            if(cur.p + memoTable[j] > memoTable[i]) {
                memoTable[i] = memoTable[j] + cur.p;
                endIndex[i] = cur.r;
            }
        }
        return memoTable[n];
    }

    public static void main(String[] args) throws IOException {
        File inputFile = new File(args[0]);
        File outputFile = new File(inputFile.getParentFile(), "test1_18010078.out");
        Scanner sc = new Scanner(inputFile);

        int activityCount = sc.nextInt();
        List<Activity> timeList = new ArrayList<>(activityCount);
        while(activityCount-- > 0) {
            int l = sc.nextInt(), r = sc.nextInt(), p = sc.nextInt();
            timeList.add(new Activity(l, r, p));
        }
        sc.close();

        int maxProfit = selectActivities(timeList);
        System.out.println(maxProfit);
        FileWriter answerWriter = new FileWriter(outputFile);
        answerWriter.write(""+maxProfit);
        answerWriter.close();
    }
}
