package li.mtt;

import java.lang.reflect.Method;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		// Create a Parser
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("h", "help", false, "Print this usage information");
		options.addOption("c", "call", true, "call functions");

		// print usage
		HelpFormatter formatter = new HelpFormatter();

		try {
			// Parse the program arguments
			CommandLine commandLine = parser.parse(options, args);

			// Set the appropriate variables based on supplied options
			String callf = "";

			if (commandLine.hasOption('c')) {
				callf = commandLine.getOptionValue('c');
				System.out.println("call functions:" + callf);
				int called = 0;
				try {
					Class<?> c = Class.forName("li.mtt.App");
					Method[] m = c.getMethods();
					for (int i = 0; i < m.length; i++) {
						if (m[i].getName().equals(callf)) {
							m[i].invoke(null);
							called = 1;
						}
					}
					if (called == 0){
						for (int i = 0; i < m.length; i++) {
							System.out.println(m[i].getName());
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else if (commandLine.hasOption('h')) {
				System.out.println("Help Message");
				formatter.printHelp("java -jar XXX.jar", options);
				System.out.println();
				System.exit(0);
			} else {
				formatter.printHelp("java -jar XXX.jar", options);
				System.exit(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void doit() {

		System.out.println("in doit");
	}
}
