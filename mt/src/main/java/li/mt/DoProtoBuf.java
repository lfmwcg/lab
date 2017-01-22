package li.mt;

import java.lang.reflect.Method;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.google.protobuf.InvalidProtocolBufferException;

import li.mt.PersonProbuf.Person;

public class DoProtoBuf {
	
	public static void doProtoBuf() {
        //模拟将对象转成byte[]，方便传输
        PersonProbuf.Person.Builder builder = PersonProbuf.Person.newBuilder();
        builder.setId(1);
        builder.setName("ant");
        builder.setEmail("ghb@soecode.com");
        PersonProbuf.Person person = builder.build();
        System.out.println("before :"+ person.toString());

        System.out.println("===========Person Byte==========");
        for(byte b : person.toByteArray()){
            System.out.print(b);
        }
        System.out.println();
        System.out.println(person.toByteString());
        System.out.println("================================");

        //模拟接收Byte[]，反序列化成Person类
        byte[] byteArray =person.toByteArray();
        Person p2;
		try {
			p2 = Person.parseFrom(byteArray);
			System.out.println("after :" +p2.toString());
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
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
					Class<?> c = Class.forName("li.mt.DoProtoBuf");
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


}
