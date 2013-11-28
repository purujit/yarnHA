import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Map with key = State Transition & value = List of application ids which
 * underwent that state transition For calculating probability just get size of
 * the list corresponding to the state transition.
 */
public class StateTransitionCounter {

	private Map<String, ArrayList<String>> stateTransitionMap = new HashMap<String, ArrayList<String>>();

	public StateTransitionCounter(String type) {

		if (type.equalsIgnoreCase("application")) {
			buildAppStateTransitionMap();
		}

	}

	public void buildAppStateTransitionMap() {

		// Transitions from "NEW" state
		stateTransitionMap.put(Utils.transitionAsString("NEW", "NEW_SAVING"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("NEW", "SUBMITTED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("NEW", "KILLED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("NEW", "FAILED"),
				new ArrayList<String>());

		// Transitions from "NEW_SAVING" state
		stateTransitionMap.put(
				Utils.transitionAsString("NEW_SAVING", "SUBMITTED"),
				new ArrayList<String>());
		stateTransitionMap.put(
				Utils.transitionAsString("NEW_SAVING", "KILLED"),
				new ArrayList<String>());
		stateTransitionMap.put(
				Utils.transitionAsString("NEW_SAVING", "FAILED"),
				new ArrayList<String>());

		// Transitions from "SUBMITTED" state
		stateTransitionMap.put(
				Utils.transitionAsString("SUBMITTED", "SUBMITTED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("SUBMITTED", "FAILED"),
				new ArrayList<String>());
		stateTransitionMap.put(
				Utils.transitionAsString("SUBMITTED", "ACCEPTED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("SUBMITTED", "KILLED"),
				new ArrayList<String>());

		// Transitions from "ACCEPTED" state
		stateTransitionMap.put(
				Utils.transitionAsString("ACCEPTED", "ACCEPTED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("ACCEPTED", "RUNNING"),
				new ArrayList<String>());
		stateTransitionMap.put(
				Utils.transitionAsString("ACCEPTED", "SUBMITTED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("ACCEPTED", "FAILED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("ACCEPTED", "KILLED"),
				new ArrayList<String>());

		// Transitions from "RUNNING" state
		stateTransitionMap.put(Utils.transitionAsString("RUNNING", "RUNNING"),
				new ArrayList<String>());
		stateTransitionMap.put(
				Utils.transitionAsString("RUNNING", "FINISHING"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("RUNNING", "FINISHED"),
				new ArrayList<String>());
		stateTransitionMap.put(
				Utils.transitionAsString("RUNNING", "SUBMITTED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("RUNNING", "FAILED"),
				new ArrayList<String>());
		stateTransitionMap.put(Utils.transitionAsString("RUNNING", "KILLED"),
				new ArrayList<String>());

		// Transitions from "FINISHING" state
		stateTransitionMap.put(
				Utils.transitionAsString("FINISHING", "FINISHED"),
				new ArrayList<String>());

	}

	public void printMap() {

		System.out.println("Printing state transition map");
		Set<String> stateTrans = stateTransitionMap.keySet();

		for (String s : stateTrans) {
			List<String> applicationList = stateTransitionMap.get(s);
			System.out.print(s + ": ");
			for (String app : applicationList) {
				System.out.print(app + " ");
			}
			System.out.println("\n");
		}

	}

	public Map<String, ArrayList<String>> getStateTransitionMap() {
		return stateTransitionMap;
	}

	public void setStateTransitionMap(
			Map<String, ArrayList<String>> stateTransitionMap) {
		this.stateTransitionMap = stateTransitionMap;
	}
}
