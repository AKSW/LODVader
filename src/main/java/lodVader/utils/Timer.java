package lodVader.utils;

public class Timer {
	
	private double startTime = 0;
	
	public void startTimer(){
		if (startTime == 0)
			startTime = System.currentTimeMillis();
	}
	
	public String stopTimer(){
		double time = System.currentTimeMillis()-startTime;
		startTime = 0;
		return String.format("%.4f", time/1000);
		
	}
	
}
