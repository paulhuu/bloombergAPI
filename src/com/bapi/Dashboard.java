package com.bapi;

import com.utils.MessageConsole;
import com.utils.Utils;
import org.jdatepicker.impl.JDatePanelImpl;
import org.jdatepicker.impl.JDatePickerImpl;
import org.jdatepicker.impl.UtilDateModel;

import javax.swing.*;
import javax.swing.JFormattedTextField.AbstractFormatter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Properties;

public class Dashboard extends JPanel {
    private JButton runhistoricalButton;
    private JPanel mainPanel;
    private JTabbedPane tab1;
    private JTextArea textArea;
    private JPanel topPanel;
    private JButton runTodayButton;
    private JDatePickerImpl leftDatePicker;
    private JDatePickerImpl rightDatePicker;
    private JLabel todayDateLabel;
    private JTextField todayDateTextField;
    private JTextField tickerField;
    private JScrollPane scrollPanel;
    private JFrame frame;

    public Dashboard() {

        runTodayButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                //JOptionPane.showMessageDialog(null,"Running for today");
                LocalDate today = LocalDate.now();
                write("Run Bloomberg report for today: " + Utils.formatDate(today));
                todayDateLabel.setText(Utils.formatDate(today));
                String tickerName = tickerField.getText().trim();
                BloombergReport br = new BloombergReport(tickerName, BloombergReport.RUN_TYPE_TODAY);
                br.runCustomReport();
            }
        });

        runhistoricalButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //JOptionPane.showMessageDialog(null,"Running for today");
                LocalDate today = LocalDate.now();
                Utils.write("Run Bloomberg historical report: " + Utils.formatDate(today));

                LocalDate leftDate =  LocalDate.parse(leftDatePicker.getJFormattedTextField().getText());
                LocalDate rightDate = LocalDate.parse(rightDatePicker.getJFormattedTextField().getText());
                write("Start Date: " + leftDate.toString() + " | " + "End Date: " + rightDate.toString());

                todayDateLabel.setText(Utils.formatDate(today));
                String tickerName = tickerField.getText().trim();

                BloombergReport br = new BloombergReport(tickerName,BloombergReport.RUN_TYPE_HISTORICAL,leftDate,rightDate);
                br.runCustomReport();
            }
        });

    }


    public void initialize(){
        frame = new JFrame("Dashboard");
        frame.setContentPane(new Dashboard().mainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setBounds(50,50,600,800);
        //frame.setSize(400,600);

        frame.setVisible(true);
        //frame.addWindowListener(createWindowListener(frame));

    }

    public void write(String msg){
        msg = msg + "\n" ;
        textArea.append(msg);
    }

    private void createUIComponents() {
        UtilDateModel leftModel = new UtilDateModel();
        UtilDateModel rightModel = new UtilDateModel();
        LocalDate rightDate = LocalDate.now();
        LocalDate leftDate = LocalDate.now().minusMonths(2);

        leftModel.setDate(leftDate.getYear(),leftDate.getMonthValue()-1,leftDate.getDayOfMonth());
        rightModel.setDate(rightDate.getYear(),rightDate.getMonthValue()-1,rightDate.getDayOfMonth());
        leftModel.setSelected(true);
        rightModel.setSelected(true);

        Properties leftProperties = new Properties();
        leftProperties.put("text.today", "Today");
        leftProperties.put("text.month", "Month");
        leftProperties.put("text.year", "Year");

        Properties rightProperties = new Properties();
        rightProperties.put("text.today", "Today");
        rightProperties.put("text.month", "Month");
        rightProperties.put("text.year", "Year");

        JDatePanelImpl leftDatePanel = new JDatePanelImpl(leftModel,leftProperties);
        JDatePanelImpl rightDatePanel = new JDatePanelImpl(rightModel,rightProperties);
        leftDatePicker = new JDatePickerImpl(leftDatePanel, new DateLabelFormatter());
        rightDatePicker = new JDatePickerImpl(rightDatePanel, new DateLabelFormatter());

        todayDateLabel = new JLabel("FINALLY");
        todayDateLabel.setText("FINALLY");
        //frame.add(todayDateLabel);

        todayDateTextField = new JTextField(Utils.formatDate(LocalDate.now()));
        todayDateTextField.setEditable(false);

        textArea = new JTextArea("");
        MessageConsole mc = new MessageConsole(textArea);
        mc.redirectOut();
        mc.redirectErr(Color.BLUE,null);
    }

    public class DateLabelFormatter extends AbstractFormatter {

        private String datePattern = "yyyy-MM-dd";
        private SimpleDateFormat dateFormatter = new SimpleDateFormat(datePattern);

        @Override
        public Object stringToValue(String text) throws ParseException {
            return dateFormatter.parseObject(text);
        }

        @Override
        public String valueToString(Object value) throws ParseException {
            if (value != null) {
                Calendar cal = (Calendar) value;
                return dateFormatter.format(cal.getTime());
            }

            return "";
        }
    }
}
