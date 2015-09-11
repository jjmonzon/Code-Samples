package StunDogMacro;

import javax.swing.*;
import java.io.InputStream;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.event.KeyEvent.*;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;

public class StunDogMacro {

	private static InputType BLACKBLOODKEYBIND;
	private static InputType HAUNTKEYBIND;
	private static InputType ZOMBIEDOGSKEYBIND;
	private static InputType BBVKEYBIND;
	private static int stunInterval = 1058;
	private static final int SLEEPTIME = 100;
	private static final int WIDTH = 400;
	private static final int HEIGHT = 200;
	private static final String CONFIG_FILE = "config.txt";
	// private static final String AUTOIT_PATH = "C:/Users/Josh/Desktop/Macro/";
	private static final String AUTOIT_PATH = "";
	private static final String AUTOIT_EXEC = "AutoItStopper.exe";
	protected static final CharSequence EXIT = "exit";
	protected static final CharSequence START = "start";
	protected static final CharSequence PAUSE = "pause";
	private static volatile boolean paused = true;
	private static Process proc = null;
	private static JTextField areaTF = new JTextField();

	public StunDogMacro() {
		StunDogMacroUI ui = new StunDogMacroUI();
	}

	public static void main(String[] args) throws IOException {
		getConfigData();
		Runtime rt = Runtime.getRuntime();
		StunDogMacro sdm = new StunDogMacro();

		try {
			proc = rt.exec(AUTOIT_PATH + AUTOIT_EXEC);
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}
		InputStream iStream = proc.getInputStream();
		InputStreamReader isr = new InputStreamReader(iStream);
		final BufferedReader bufReader = new BufferedReader(isr);

		OutputStream oStream = proc.getOutputStream();
		final PrintWriter pw = new PrintWriter(oStream, true);

		Runnable bufReaderRunnable = new Runnable() {
			public void run() {
				String output;

				try {
					while ((output = bufReader.readLine()) != null) {
						if (output.toLowerCase().contains(EXIT)) {
							proc.destroy();
							System.exit(0);
						} else if (output.toLowerCase().contains(START)) {
							setStatus("Macro started by user!");
							paused = false;
							Thread rgStunner = new Thread(myRun);
							rgStunner.start();
						} else if (output.toLowerCase().contains(PAUSE)) {
							setStatus("Paused by user!");
							paused = true;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (bufReader != null) {
						try {
							bufReader.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}

			Runnable myRun = new Runnable() {
				public void run() {
					long runningTime = 15001;
					while (!paused) {
						try {
							if (runningTime > 15000) {
								runningTime = 0;
								castBBV();
							}
							long startTime = System.currentTimeMillis();
							runMacro();
							long estimatedTime = System.currentTimeMillis()
									- startTime;
							runningTime = runningTime + estimatedTime;

						} catch (AWTException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					setStatus("Macro stopped.");
				}
			};
		};
		new Thread(bufReaderRunnable).start();

	}

	public static void setStatus(String status) {
		System.out.println(status);
		areaTF.setText(status);
	}

	public static void runMacro() throws AWTException, InterruptedException {
		summonDogs();
		Thread.sleep(360);
		castBlackBlood();
		Thread.sleep(stunInterval - 710);
		summonDogs();
		Thread.sleep(260);
		castHaunt();
		Thread.sleep(450);
		castBlackBlood();
		Thread.sleep(stunInterval - 810);
		castHaunt();
		Thread.sleep(450);
	}

	public static void castBlackBlood() throws AWTException,
			InterruptedException {
		Robot bot = new Robot();
		PointerInfo a = MouseInfo.getPointerInfo();
		Point b = a.getLocation();
		int x = (int) b.getX();
		int y = (int) b.getY();
		bot.keyPress(KeyEvent.VK_SHIFT);
		if (BLACKBLOODKEYBIND.type.equals("MouseEvent")) {
			bot.mousePress(BLACKBLOODKEYBIND.robotInteger);
			bot.mouseRelease(BLACKBLOODKEYBIND.robotInteger);
		} else if (BLACKBLOODKEYBIND.type.equals("KeyEvent")) {
			bot.keyPress(BLACKBLOODKEYBIND.robotInteger);
			bot.keyRelease(BLACKBLOODKEYBIND.robotInteger);
		}
		bot.keyRelease(KeyEvent.VK_SHIFT);
		setStatus("Casting Sacrifice");

	}

	public static void castHaunt() throws AWTException, InterruptedException {
		Robot bot = new Robot();
		PointerInfo a = MouseInfo.getPointerInfo();
		Point b = a.getLocation();
		int x = (int) b.getX();
		int y = (int) b.getY();

		if (HAUNTKEYBIND.type.equals("MouseEvent")) {
			bot.mousePress(HAUNTKEYBIND.robotInteger);
			bot.mouseRelease(HAUNTKEYBIND.robotInteger);
		} else if (HAUNTKEYBIND.type.equals("KeyEvent")) {
			bot.keyPress(HAUNTKEYBIND.robotInteger);
			bot.keyRelease(HAUNTKEYBIND.robotInteger);
		}
		setStatus("Casting Haunt");

	}

	public static void summonDogs() throws AWTException, InterruptedException {
		Robot bot = new Robot();
		if (ZOMBIEDOGSKEYBIND.type.equals("MouseEvent")) {
			bot.mousePress(ZOMBIEDOGSKEYBIND.robotInteger);
			bot.mouseRelease(ZOMBIEDOGSKEYBIND.robotInteger);
		} else if (ZOMBIEDOGSKEYBIND.type.equals("KeyEvent")) {
			bot.keyPress(ZOMBIEDOGSKEYBIND.robotInteger);
			bot.keyRelease(ZOMBIEDOGSKEYBIND.robotInteger);
		}
		setStatus("Summon Dogs");

	}

	public static void castBBV() throws AWTException, InterruptedException {
		Robot bot = new Robot();
		if (BBVKEYBIND.type.equals("MouseEvent")) {
			bot.mousePress(BBVKEYBIND.robotInteger);
			bot.mouseRelease(BBVKEYBIND.robotInteger);
		} else if (BBVKEYBIND.type.equals("KeyEvent")) {
			bot.keyPress(BBVKEYBIND.robotInteger);
			bot.keyRelease(BBVKEYBIND.robotInteger);
		}
		setStatus("Casting BBV");

	}

	public class StunDogMacroUI extends JFrame {
		private static final int WIDTH = 340;
		private static final int HEIGHT = 40;
		private JLabel instructions0L, instructions1L, instructions2L,
				instructions3L, statusL, areaL;

		public StunDogMacroUI() {
			instructions1L = new JLabel(
					"F9 - Pause Macro | F10 - Start Macro | F11 - Stop Macro",
					SwingConstants.LEFT);
			instructions1L.setOpaque(false);
			instructions1L.setForeground(Color.white);
			statusL = new JLabel("Status: ", SwingConstants.LEFT);
			statusL.setForeground(Color.white);
			statusL.setOpaque(false);
			areaTF.setEditable(false);
			areaTF.setBorder(null);
			areaTF.setForeground(Color.white);
			areaTF.setText("Paused");
			areaTF.setOpaque(false);
			areaTF.setBackground(new Color(0,0,0,0));
			Dimension screenDim = java.awt.Toolkit.getDefaultToolkit().getScreenSize();
			this.setLocation((screenDim.width-350)/2, 0);
			setUndecorated(true);
			//this.setLocation(1800, 200);
			//setLayout(new FlowLayout());
			setAlwaysOnTop(true);
			// Without this, the window is draggable from any non transparent
			// point, including points inside textboxes.
			getRootPane().putClientProperty(
					"apple.awt.draggableWindowBackground", true);
			JPanel p0=new JPanel();
			p0.setOpaque(false);
			p0.setBackground(new Color(0,0,0,0));
			p0.setLayout(new BoxLayout(p0, BoxLayout.PAGE_AXIS));
			JPanel p1=new JPanel();
			add(p0);
			p0.add(p1);
			
			p1.setOpaque(false);
			p1.setBackground(new Color(0,0,0,0));
			//Container pane = getContentPane();
			p1.setLayout(new BoxLayout(p1, BoxLayout.LINE_AXIS));
			p1.add(instructions1L);
			JPanel p2=new JPanel();
			p2.setLayout(new BoxLayout(p2, BoxLayout.LINE_AXIS));
			p2.setOpaque(false);
			p2.setBackground(new Color(0,0,0,0));
			p2.add(statusL);
			p2.add(areaTF);
			p0.add(p2);
			setBackground(new Color(120,122,122,50));
			setSize(WIDTH, HEIGHT);
			setVisible(true);
			setDefaultCloseOperation(EXIT_ON_CLOSE);
			}
	}

	public static class InputType {
		int robotInteger;
		String type;

		public InputType(Integer robotInteger, String type) {
			this.robotInteger = robotInteger;
			this.type = type;
		}
	}

	public static InputType convertToRobotInteger(String character) {
		switch (character) {
		case "a":
			return new InputType(KeyEvent.VK_A, "KeyEvent");
		case "b":
			return new InputType(KeyEvent.VK_B, "KeyEvent");
		case "c":
			return new InputType(KeyEvent.VK_C, "KeyEvent");
		case "d":
			return new InputType(KeyEvent.VK_D, "KeyEvent");
		case "e":
			return new InputType(KeyEvent.VK_E, "KeyEvent");
		case "f":
			return new InputType(KeyEvent.VK_F, "KeyEvent");
		case "g":
			return new InputType(KeyEvent.VK_G, "KeyEvent");
		case "h":
			return new InputType(KeyEvent.VK_H, "KeyEvent");
		case "i":
			return new InputType(KeyEvent.VK_I, "KeyEvent");
		case "j":
			return new InputType(KeyEvent.VK_J, "KeyEvent");
		case "k":
			return new InputType(KeyEvent.VK_K, "KeyEvent");
		case "l":
			return new InputType(KeyEvent.VK_L, "KeyEvent");
		case "m":
			return new InputType(KeyEvent.VK_M, "KeyEvent");
		case "n":
			return new InputType(KeyEvent.VK_N, "KeyEvent");
		case "o":
			return new InputType(KeyEvent.VK_O, "KeyEvent");
		case "p":
			return new InputType(KeyEvent.VK_P, "KeyEvent");
		case "q":
			return new InputType(KeyEvent.VK_Q, "KeyEvent");
		case "r":
			return new InputType(KeyEvent.VK_R, "KeyEvent");
		case "s":
			return new InputType(KeyEvent.VK_S, "KeyEvent");
		case "t":
			return new InputType(KeyEvent.VK_T, "KeyEvent");
		case "u":
			return new InputType(KeyEvent.VK_U, "KeyEvent");
		case "v":
			return new InputType(KeyEvent.VK_V, "KeyEvent");
		case "w":
			return new InputType(KeyEvent.VK_W, "KeyEvent");
		case "x":
			return new InputType(KeyEvent.VK_X, "KeyEvent");
		case "y":
			return new InputType(KeyEvent.VK_Y, "KeyEvent");
		case "z":
			return new InputType(KeyEvent.VK_Z, "KeyEvent");
		case "0":
			return new InputType(KeyEvent.VK_0, "KeyEvent");
		case "1":
			return new InputType(KeyEvent.VK_1, "KeyEvent");
		case "2":
			return new InputType(KeyEvent.VK_2, "KeyEvent");
		case "3":
			return new InputType(KeyEvent.VK_3, "KeyEvent");
		case "4":
			return new InputType(KeyEvent.VK_4, "KeyEvent");
		case "5":
			return new InputType(KeyEvent.VK_5, "KeyEvent");
		case "6":
			return new InputType(KeyEvent.VK_6, "KeyEvent");
		case "7":
			return new InputType(KeyEvent.VK_7, "KeyEvent");
		case "8":
			return new InputType(KeyEvent.VK_8, "KeyEvent");
		case "9":
			return new InputType(KeyEvent.VK_9, "KeyEvent");
		case "-":
			return new InputType(KeyEvent.VK_MINUS, "KeyEvent");
		case "=":
			return new InputType(KeyEvent.VK_EQUALS, "KeyEvent");
		case "!":
			return new InputType(KeyEvent.VK_EXCLAMATION_MARK, "KeyEvent");
		case "@":
			return new InputType(KeyEvent.VK_AT, "KeyEvent");
		case "#":
			return new InputType(KeyEvent.VK_NUMBER_SIGN, "KeyEvent");
		case "$":
			return new InputType(KeyEvent.VK_DOLLAR, "KeyEvent");
		case "^":
			return new InputType(KeyEvent.VK_CIRCUMFLEX, "KeyEvent");
		case "&":
			return new InputType(KeyEvent.VK_AMPERSAND, "KeyEvent");
		case "*":
			return new InputType(KeyEvent.VK_ASTERISK, "KeyEvent");
		case "(":
			return new InputType(KeyEvent.VK_LEFT_PARENTHESIS, "KeyEvent");
		case ")":
			return new InputType(KeyEvent.VK_RIGHT_PARENTHESIS, "KeyEvent");
		case "_":
			return new InputType(KeyEvent.VK_UNDERSCORE, "KeyEvent");
		case "+":
			return new InputType(KeyEvent.VK_PLUS, "KeyEvent");
		case "\t":
			return new InputType(KeyEvent.VK_TAB, "KeyEvent");
		case "\n":
			return new InputType(KeyEvent.VK_ENTER, "KeyEvent");
		case "[":
			return new InputType(KeyEvent.VK_OPEN_BRACKET, "KeyEvent");
		case "]":
			return new InputType(KeyEvent.VK_CLOSE_BRACKET, "KeyEvent");
		case "\\":
			return new InputType(KeyEvent.VK_BACK_SLASH, "KeyEvent");
		case ";":
			return new InputType(KeyEvent.VK_SEMICOLON, "KeyEvent");
		case ":":
			return new InputType(KeyEvent.VK_COLON, "KeyEvent");
		case "\"":
			return new InputType(KeyEvent.VK_QUOTE, "KeyEvent");
		case ",":
			return new InputType(KeyEvent.VK_COMMA, "KeyEvent");
		case ".":
			return new InputType(KeyEvent.VK_PERIOD, "KeyEvent");
		case "/":
			return new InputType(KeyEvent.VK_SLASH, "KeyEvent");
		case " ":
			return new InputType(KeyEvent.VK_SPACE, "KeyEvent");
		case "MOUSE1":
			return new InputType(InputEvent.BUTTON1_MASK, "MouseEvent");
		case "MOUSE2":
			return new InputType(InputEvent.BUTTON2_MASK, "MouseEvent");
		case "MOUSE3":
			return new InputType(InputEvent.BUTTON3_MASK, "MouseEvent");
		default:
			throw new IllegalArgumentException("Cannot recognize input type "
					+ character);
		}
	}

	public static void getConfigData() throws IOException {
		FileReader fileReader = new FileReader(CONFIG_FILE);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		StringBuffer stringBuffer = new StringBuffer();
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			String[] input = line.split("=");
			switch (input[0]) {
			case "Sacrifice":
				BLACKBLOODKEYBIND = convertToRobotInteger(input[1]);
				System.out.println("BLACKBLOODKEYBIND set to: " + input[1]);
				break;
			case "Haunt":
				HAUNTKEYBIND = convertToRobotInteger(input[1]);
				System.out.println("HAUNTKEYBIND set to: " + input[1]);
				break;
			case "ZombieDogs":
				ZOMBIEDOGSKEYBIND = convertToRobotInteger(input[1]);
				System.out.println("ZOMBIEDOGSKEYBIND set to: " + input[1]);
				break;
			case "BBV":
				BBVKEYBIND = convertToRobotInteger(input[1]);
				System.out.println("BBVKEYBIND set to: " + input[1]);
				break;
			case "StunInterval":
				stunInterval = Integer.parseInt(input[1]);
				System.out.println("stunInterval set to: " + input[1]);
				break;
			default:
				break;
			}

		}
		fileReader.close();
	}
}
