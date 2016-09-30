import java.io.IOException;

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
//		DataPreProcessor dpp = new DataPreProcessor();
//		GenerateCoMatrix gCoMatrix = new GenerateCoMatrix();
		if(args.length < 5){
			System.out.println("Input Usage:<WatchedHistory> <tmpoutput> <comatrix> <comatrix/part-r-00000> <recommendation>");
			System.exit(1);
		}
		String[] path1 = {args[0], args[1]};
		String[] path2 = {args[1], args[2]};
		String[] path3 = {args[3], args[0], args[4]};
		
		DataPreProcessor.main(path1);
		GenerateCoMatrix.main(path2);
		Multiplication.main(path3);
	}

}
