

public class Utils {


	public static String transitionAsString(String fromState, String toState) {

		StringBuilder stateTransition = new StringBuilder(fromState + "to"
				+ toState);
		return stateTransition.toString();
	}
}
