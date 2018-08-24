import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class InputHelper {

    /**
     * Gets user input from console.
     * @param prompt
     * @return
     */
    public static String getInput(String prompt) {
        BufferedReader stdin = new BufferedReader(
                new InputStreamReader(System.in));

        System.out.print(prompt);
        System.out.flush();

        try {
            return stdin.readLine();
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    public static double getDoubleInput(String prompt) throws NumberFormatException {
        String input = getInput(prompt);
        return Double.parseDouble(input);
    }

    /**
     * Checks if user input is in a valid date format.
     * @param prompt
     * @return
     */
    public static String getDateInput(String prompt) {
        try {
            SimpleDateFormat timeformatter = new SimpleDateFormat("yyyy-MM-dd");
            String input = getInput(prompt);
            timeformatter.parse(input); //to check if date input is valid, will throw exception if it is incorrect
            return input;
        } catch (ParseException e) {
            System.out.println("Invalid date input");
            return null;
        }
    }
}
