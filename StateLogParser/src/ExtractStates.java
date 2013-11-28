import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExtractStates {

	private BufferedReader br;
	private StateTransitionCounter stateCounter;
	private Map<String, ArrayList<String>> stateTransitionMap;

	public ExtractStates(String fileName) {

		stateCounter = new StateTransitionCounter("application");
		stateTransitionMap = stateCounter.getStateTransitionMap();

		try {

			br = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void parseFile() {
		try {

			String currentLineString;
			String rmAppStateTransition = null;

			while ((currentLineString = br.readLine()) != null) {

				if (currentLineString.contains("State change")
						& currentLineString.contains("RMAppImpl")) {
					rmAppStateTransition = currentLineString;
					addStateTransitionToMap(currentLineString);
				}

			}

			stateCounter.printMap();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public void addStateTransitionToMap(String logEntry) {

		String[] tokens = logEntry.split(" ");
		String applicationId = null;
		String fromState = null;
		String toState = null;
		String previousWord = " ";

		for (String s : tokens) {

			if (s.contains("application_")) {
				applicationId = s;
			}

			if (previousWord.equalsIgnoreCase("from")) {
				fromState = s;
			} else if (previousWord.equalsIgnoreCase("to")) {
				toState = s;
			}
			previousWord = s;
		}

		List<String> applicationList = stateTransitionMap.get(Utils
				.transitionAsString(fromState, toState));
		if (!applicationList.contains(applicationId)) {
			applicationList.add(applicationId);
		}
	}

	public static void main(String[] args) {

		ExtractStates extractStates = new ExtractStates(args[0]);
		extractStates.parseFile();
	}
}
