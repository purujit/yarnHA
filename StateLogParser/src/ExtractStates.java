import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Find specific log entries with the arguments: log_fileName, Date of the
 * entry(Format: yyyy-mm-dd) in log file, start and end time entries between
 * which log needs to be parsed. This mode can be turned on/off depending on the
 * last argument true/false.
 * 
 * @author sakshi
 * 
 */
public class ExtractStates {

	private BufferedReader br;
	private StateTransitionCounter stateCounter;
	private Map<String, ArrayList<String>> stateTransitionMap;
	private boolean doFindSpecificEntries = false;
	private Date logStartTime;
	private Date logEndTime;

	public ExtractStates(String fileName, String logDate, String startTime,
			String endTime, boolean doFindSpecificEntries) {

		if (doFindSpecificEntries) {
			SimpleDateFormat parser = new SimpleDateFormat(
					"yyyy-mm-dd HH:mm:ss");

			try {
				this.logStartTime = parser.parse(logDate + " " + startTime);
				this.logEndTime = parser.parse(logDate + " " + endTime);
				this.doFindSpecificEntries = true;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

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

	public void findEntries(String[] tokens) {
		String applicationId = null;
		String fromState = null;
		String toState = null;
		String previousWord = " ";

		for (String s : tokens) {

			// System.out.println(s);
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

	public void addStateTransitionToMap(String logEntry) {

		String[] tokens = logEntry.split(" ");
		String dateOfEntry = tokens[0];
		String timeOfEntry = tokens[1].split(",")[0];
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
		try {
			Date logTimeEntry = parser.parse(dateOfEntry + " " + timeOfEntry);

			if (doFindSpecificEntries && logTimeEntry.after(logStartTime)
					&& logTimeEntry.before(logEndTime)) {
				findEntries(tokens);
			}
			if (!doFindSpecificEntries) {
				findEntries(tokens);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public static void main(String[] args) {

		 ExtractStates extractStates = new ExtractStates(args[0], args[1], args[2], args[3], Boolean.valueOf(args[4]));
		
		 //example
//		ExtractStates extractStates = new ExtractStates(
//				"/home/sakshi/workspace/hadoop/StateLogParser2/src/Resource/yarn-sakshi-resourcemanager-ubuntusakshi.log",
//				"2013-11-27", "22:00:00", "23:00:00", true);
		extractStates.parseFile();
	}
}
