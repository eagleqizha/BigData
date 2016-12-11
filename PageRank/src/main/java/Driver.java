
/**
 * Created by qianzhang on 10/6/16.
 */
public class Driver {
    public static void main(String[] args) throws Exception{
        String tranMapper = args[0];
        String prMapper = args[1];
        // relation.txt = args[0]
        // pr0 = args[1];
        //firstreduceOutput = args[2]
        // times of convergence = args[3]
        //run MR1, MR2
        //iteration

        String MR1 = args[2];
        int count = Integer.parseInt(args[3]);
        String beta = args[4];

        for(int i = 0; i < count; i++) {
            //prMatrix = args[1] = "pr" prMatrix + i = "pr" + "0" = "pr0"
            String[] args1 = {tranMapper, prMapper + i, MR1 + i};
            UnitMultiplication.main(args1);
            String[] args2 = {MR1 + i, prMapper + 0, prMapper + (i + 1),  beta};
            UnitSum.main(args2);
        }
    }

}
