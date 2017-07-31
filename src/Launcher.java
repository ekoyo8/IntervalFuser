import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;

public class Launcher {
	public static void main(String[] args) {
		inputOutput();
	}
	
	private static void inputOutput() {
		TreeMap<String, String> commands = new TreeMap<>();
		commands.put(".help", "Lists available commands.");
		commands.put(".fuse", "Fuses intervals.");
		commands.put(".config", "Displays the current configuration of the IntervalFuser.");
		commands.put(".quit", "Ends the program.");
		commands.put(".exit", "Ends the program.");
		commands.put(".startlimit", "Sets lower limit of intervals. (yyyy-MM-dd)");
		commands.put(".endlimit", "Sets the upper limit of intervals. (yyyy-MM-dd)");
		commands.put(".entitystart", "Sets the lower limit of id's included in fused intervals. (Integer)");
		
		IntervalFuser iFuser = new IntervalFuser();
		InputStreamReader inStreamReader = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(inStreamReader);
		System.out.println("IntervalFuser ready... (.help for a list of commands)");
		String input = "";
		while (!input.equals("exit")) {
			input = "";
			try {
				input = br.readLine();
				if (input.startsWith(".startlimit ")) {
					iFuser.setStartLimit(input.substring(input.indexOf(" ")+1) + " 00:00:00.000000");
					continue;
				}
				
				if (input.startsWith(".endlimit ")) {
					iFuser.setEndLimit(input.substring(input.indexOf(" ")+1) + " 00:00:00.000000");
					continue;
				}
				
				if (input.startsWith(".entitystart ")) {
					try {
						iFuser.setIdLimit(Integer.parseInt(input.substring(input.indexOf(" ")+1)));
					} catch (Exception e) {
						System.out.println("Bad argument..help (Integer required)");
					}
					continue;
				}
				
				switch (input) {
				case ".help":
					for (String command : commands.keySet()) {
						System.out.println(command + " | " + commands.get(command));
					}
					break;
				case ".config":
					System.out.println("startlimit: " + iFuser.getStartLimit());
					System.out.println("endlimit: " + iFuser.getEndLimit());
					System.out.println("entitystart: " + iFuser.getIdLimit());
					break;
				case ".fuse":
					iFuser.fuseIntervals();
					break;
				case ".exit":
				case ".quit":
					input = "exit";
					break;
				default:
					System.out.println("Unknown command. (.help for a list of commands.)");
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
