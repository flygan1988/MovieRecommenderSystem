
public class MovieRelation {
	private int movieA;
	private int movieB;
	private int relation;
	
	public MovieRelation(int movieA, int movieB, int relation){
		this.movieA = movieA;
		this.movieB = movieB;
		this.relation = relation;
	}

	public int getMovieA() {
		return movieA;
	}

	public void setMovieA(int movieA) {
		this.movieA = movieA;
	}

	public int getMovieB() {
		return movieB;
	}

	public void setMovieB(int movieB) {
		this.movieB = movieB;
	}

	public int getRelation() {
		return relation;
	}

	public void setRelation(int relation) {
		this.relation = relation;
	}
}
