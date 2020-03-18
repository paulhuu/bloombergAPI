package com.bapi;

import com.utils.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.dizitart.no2.Cursor;
import org.dizitart.no2.FindOptions;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.SortOrder;
import org.dizitart.no2.objects.Id;
import org.dizitart.no2.objects.ObjectRepository;
import org.dizitart.no2.objects.filters.ObjectFilters;

import java.io.*;
import java.time.LocalDate;
import java.util.*;

public class BloombergReport{

    public static BloombergAPI BLOOMBERG_API;
    public static Integer CONFIG_DAYS_IN_DB = 60;
    public static Integer CONFIG_HISTORICAL_DATA_BATCH_SIZE = 50;
    public static Integer CONFIG_HISTORICAL_DATA_BATCH_SIZE_SINGLE_DAY = 500;

    public static String PARENT_TICKER = "";
    public static Nitrite NITRITE_DB = null;
    public static ObjectRepository<SecurityObject> NITRITE_SECURITY_REPO = null;
    public static ObjectRepository<AlertObject> NITRITE_ALERT_REPO = null;
    public static final String RUN_TYPE_TODAY = "TODAY";
    public static final String RUN_TYPE_HISTORICAL = "HISTORICAL";
    public static final String PATH_DATA = "./data/";
    public static final String PATH_DB = "./db/";
    public static String RUN_TYPE = "";
    public static LocalDate LEFT_DATE;
    public static LocalDate RIGHT_DATE;
    public static Integer DATE_RANGE;

    public static final String PX_LAST = "PX_LAST";
    public static final String VOLUME_AVG_20D_TOTAL = "VOLUME_AVG_20D_TOTAL";
    public static final String THIRTY_DAY_IMPVOL = "30DAY_IMPVOL_100.0%MNY_DF";

    public static final String HEADER_DATE = "DATE";
    public static final String HEADER_20_DAY_AVERAGE = "20 DAY AVG";
    public static final String HEADER_20_DAY_STDEV = "20 DAY ST DEV";
    public static final String HEADER_EXCLUDE = "EXCLUDE";
    public static final String HEADER_ALERT_01 = "ALERT 01";
    public static final String HEADER_ALERT_01_DAYS = "ALERT 01 DAYS";
    public static final String HEADER_FILTER_01 = "FILTER 01";
    public static final String HEADER_FILTER_02 = "FILTER 02";
    public static final String HEADER_FILTER_03 = "FILTER 03";
    public static final String HEADER_COMMENT = "COMMENT";

    public static final String US_EQUITY = "US EQUITY";
    public static final String US_1M_100_VOL_LIVE_EQUITY  = "US 1M 100 VOL LIVE EQUITY";

    public static Set<String> securityAlertSet;
    public static HashMap<String,String> EXCLUDE_SECURITIES_MAP;

    public BloombergReport(String tickerName, String runType){
        if(runType.equals(RUN_TYPE_TODAY)){
            RUN_TYPE = RUN_TYPE_TODAY;
            RIGHT_DATE = LocalDate.now();
            LEFT_DATE = LocalDate.now().minusDays(1);
            DATE_RANGE = 20;
        }
        else if (runType.equals(RUN_TYPE_HISTORICAL)){
            RUN_TYPE = RUN_TYPE_HISTORICAL;
            LEFT_DATE = LocalDate.now().minusMonths(1);
            RIGHT_DATE = LocalDate.now();
            DATE_RANGE = 20;
        }
        PARENT_TICKER = tickerName;
        securityAlertSet = new HashSet<String>();
        BLOOMBERG_API = new BloombergAPI();
        initializeDB();
    }
    public BloombergReport(String tickerName, String runType, LocalDate startDate, LocalDate endDate){
        if(runType.equals(RUN_TYPE_TODAY)){
            RUN_TYPE = RUN_TYPE_TODAY;
            RIGHT_DATE = LocalDate.now();
            LEFT_DATE = LocalDate.now();
            DATE_RANGE = 20;
        }
        else if (runType.equals(RUN_TYPE_HISTORICAL)){
            RUN_TYPE = RUN_TYPE_HISTORICAL;
            LEFT_DATE = LocalDate.now().minusMonths(1);
            RIGHT_DATE = LocalDate.now();
            DATE_RANGE = 20;
        }
        PARENT_TICKER = tickerName;
        securityAlertSet = new HashSet<String>();
        initializeDB();
    }

    public static void buildSecuritiesToExcludeMap(Set<String> excludeSet01, Set<String> excludeSet02){
        EXCLUDE_SECURITIES_MAP = new HashMap<String,String>();

        for(String security: excludeSet01){
            EXCLUDE_SECURITIES_MAP.put(security, "PX_LAST below $2.00");
        }

        for(String security: excludeSet02){
            EXCLUDE_SECURITIES_MAP.put(security, "VOLUME_AVG_20D_TOTAL less than 500");
        }

    }


    public static HashMap<String,String> buildAppendedEquityMap(Set<String> tickerSet, String dataPointName){
        HashMap<String,String> updatedTickerMap = new HashMap<> ();
        String appendedTicker = "";
        for(String ticker : tickerSet){
            appendedTicker = (ticker + " " + dataPointName).trim();
            updatedTickerMap.put(appendedTicker,ticker);
        }
        return updatedTickerMap;
    }


    public static void runCustomReport(){
        BloombergAPI bApi = new BloombergAPI();
        String dataPointName = "US EQUITY";
        String average20DayTotal = "VOLUME_AVG_20D_TOTAL";
        String lastPrice = "PX_LAST";
        String oneMonthImpVol = "1M 100 VOL";
        String oneMonthImpVolHist = "30DAY_IMPVOL_100.0%MNY_DF";
        BloombergAPI.DataSetObject dso;

        ArrayList<Set<String>> filteredSetList;
        Set<String> fieldSet = new HashSet<String>();


        Map<String, HashMap<String,HashMap<String,String>>> bdpMap =new HashMap<String, HashMap<String,HashMap<String,String>>>();

        //1.Get all securities under an index
        Set<String> fullSecuritySet = bApi.getIndexMembers(PARENT_TICKER);

        if(fullSecuritySet.isEmpty()){
            Utils.write("No securities found from parent: " + PARENT_TICKER);
        }

        //Update tickers, append "US EQUITY
        HashMap<String,String> usEquitySecurityMap = buildAppendedEquityMap(fullSecuritySet,US_EQUITY);
        //key = AAPL US EQUITY, value = AAPL
        fullSecuritySet = usEquitySecurityMap.keySet();

        //2.Get Last Price
        fieldSet.add(lastPrice);
        dso = bApi.getReferenceData(fullSecuritySet,fieldSet);

        //DataSetObject is a 4D map, create flat map where key = security, value = singular field value
        HashMap<String,String> securityToLastPriceMap = new HashMap<String,String>();
        securityToLastPriceMap = createFlatMap(dso);

        //3. filter by minimum price of $2.00
        filteredSetList = filterOnPrice(securityToLastPriceMap, 2.00, null);
        Set<String> includeSet01 = filteredSetList.get(0);
        Set<String> excludeSet01 = filteredSetList.get(1);

        //4. Get The 20 Day Average Total
        fieldSet.clear();
        fieldSet.add(average20DayTotal);
        dso = bApi.getReferenceData(includeSet01,fieldSet);

        //key = security, value = price for 20 day average
        HashMap<String,String> securityTo20DayAvgMap = createFlatMap(dso);
        filteredSetList = filterOnPrice(securityTo20DayAvgMap,500.00,null);
        Set<String> includeSet02 = filteredSetList.get(0);
        Set<String> excludeSet02 = filteredSetList.get(1);

        //5. Get One Month Implied Volatility between date range
        fieldSet.clear();
        fieldSet.add(oneMonthImpVolHist);

        HashSet<String> batchedSecuritySet = new HashSet<>();
        Set<String> batchedAppendedSecuritySet = new HashSet<>();
        //Initialize EXCLUDE_SECURITIES_MAP
        buildSecuritiesToExcludeMap(excludeSet01,excludeSet02);


        //Set batch size depending on run, today = 500 securities at a time since getting 1 days worth of data
        //historical runs for 30 days worth of data , 50 securities at a time
        Integer batchSize = RUN_TYPE.equals(RUN_TYPE_TODAY) ? CONFIG_HISTORICAL_DATA_BATCH_SIZE_SINGLE_DAY : CONFIG_HISTORICAL_DATA_BATCH_SIZE;


        ArrayList<String> securityList = new ArrayList(includeSet02);
        Collections.sort(securityList);
        Utils.write("Number of securities to process: " + securityList.size());
        Integer numProcessed = 0;

        for(int i = 0; i< 10; i = i + 10){
            batchedSecuritySet.clear();
            batchedAppendedSecuritySet.clear();

            //Bulkified
            for(int j = i;  j< i+10; j++){
                if(j<securityList.size()) {
                    batchedSecuritySet.add(securityList.get(j));
                    numProcessed++;
                    Utils.write("Adding security to batch API: " + securityList.get(j));
                }
                else{break;}
            }
            //Do not process if no more securities
            if(batchedSecuritySet.isEmpty()){break;}

            HashMap<String, HashMap<String,String>> securityDataFromAPI;
            Map<String,SecurityObject> soMap = new HashMap<String,SecurityObject>();

            /****MAIN UPSERT SEQUENCE FOR TODAY RUN****/
            if(RUN_TYPE.equals(RUN_TYPE_TODAY)){

                //Convert AAPL US EQUITY to AAPL
                for(String equitySecurity: batchedSecuritySet){
                    batchedAppendedSecuritySet.add(usEquitySecurityMap.get(equitySecurity));
                }
                //Get the US_1M_100_VOL_LIVE_EQUITY field for live data
                fieldSet.clear();
                fieldSet.add(PX_LAST);

                HashMap<String,String> us1M100SecurityMap = buildAppendedEquityMap(batchedAppendedSecuritySet,US_1M_100_VOL_LIVE_EQUITY);
                batchedAppendedSecuritySet = us1M100SecurityMap.keySet();
                dso = bApi.getReferenceData(batchedAppendedSecuritySet,fieldSet);

                //key = security , value = map - k = date, v = field value
                //Create a 2D map from the 4D returned by API
                securityDataFromAPI = create2DMap(dso);

                //6. Use data from API to insert/update DB of matching securities with custom calculations
                soMap = upsertHistoricalData(us1M100SecurityMap, securityDataFromAPI,PX_LAST);
            }
            /****MAIN UPSERT SEQUENCE FOR HISTORICAL RUN****/
            else if(RUN_TYPE.equals(RUN_TYPE_HISTORICAL)){
                dso = bApi.getHistoricalData(batchedSecuritySet,fieldSet,LEFT_DATE,RIGHT_DATE);

                //key = security , value = map - k = date, v = field value
                //Create a 2D map from the 4D returned by API
                securityDataFromAPI = create2DMap(dso);

                //6. Use data from API to insert/update DB of matching securities with custom calculations
                soMap = upsertHistoricalData(securityTo20DayAvgMap, securityDataFromAPI,oneMonthImpVolHist);

            }
            //7. Print out Securities
            for(SecurityObject so : soMap.values()){
                try {
                    writeSecurityObjectToCSV(so);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            Set<String> removedSecuritySet = securityAlertSet;
            for(String security : removedSecuritySet){
                //Utils.write("Alert:" + security);
                // rso.remove(security);
            }
        }

        Utils.write("Number of securities processed: " +numProcessed);
        bApi.end();
        shutdown();
    }

    public static void shutdown(){
        NITRITE_DB.close();
    }

    public static void initializeDB(){
        String tickerDb = PARENT_TICKER.toLowerCase();
        String filePath = PATH_DB + tickerDb + ".db";
        NITRITE_DB = Nitrite.builder()
                .compressed()
                .filePath(filePath)
                .openOrCreate();

        NITRITE_SECURITY_REPO =  NITRITE_DB.getRepository(SecurityObject.class);
        NITRITE_ALERT_REPO = NITRITE_DB.getRepository(AlertObject.class);
//        // observe any change to the repository
//        NITRITE_SECURITY_REPO.register(new ChangeListener() {
//            @Override
//            public void onChange(ChangeInfo changeInfo) {
//                // your logic based on action
//            }
//        });
//        NITRITE_ALERT_REPO.register(new ChangeListener() {
//            @Override
//            public void onChange(ChangeInfo changeInfo) {
//                // your logic based on action
//            }
//        });
    }

    public static HashMap<String, HashMap<String,Set<String>>> create3DMap(BloombergAPI.DataSetObject dso){
        HashMap<String,HashMap<String,Set<String>>> securityToDatesMap = new HashMap<>();
        HashMap<String,Set<String>> fieldMap;
        Set<String> fieldValueSet;
        BloombergAPI.SecurityDataObject sdo;
        BloombergAPI.FieldDataObject fdo;
        HashMap<String,String> dataMap;

        String value = "";
        for(String security : dso.keySet()){
            sdo = dso.get(security);

            if(securityToDatesMap.containsKey(security)){
                fieldMap = securityToDatesMap.get(security);
            }
            else{
                fieldMap = new HashMap<>();
                securityToDatesMap.put(security,fieldMap);
            }

            //Field level map
            for(String fieldKey : sdo.keySet()){
                fdo = sdo.get(fieldKey);

                if(fieldMap.containsKey(fieldKey)){
                    fieldValueSet = fieldMap.get(fieldKey);
                }
                else{
                    fieldValueSet = new HashSet<>();
                    fieldMap.put(fieldKey,fieldValueSet);
                }

                //Value level map, takes into account of fields that return multiple values
                for(String fieldDataKey: fdo.keySet()){
                    dataMap = fdo.get(fieldDataKey);
                    for(String fieldValueKey : dataMap.keySet()){
                        value = dataMap.get(fieldValueKey);
                        fieldValueSet.add(value);
                    }
                }

            }
        }
        return securityToDatesMap;
    }

    public static void  filter02_Dates(HashMap<String,SecurityObject> soMap, String currentDateString, Integer daysToFilterFromStartDate){

        Set<String> securitySet = soMap.keySet();
        SecurityObject so;
        Set<String> fieldSet =  BLOOMBERG_API.buildExpirationDateSet();
        BloombergAPI.DataSetObject dso = BLOOMBERG_API.getReferenceData(securitySet,fieldSet);

        HashMap<String, HashMap<String,Set<String>>> securityFilterMap = create3DMap(dso);
        HashMap<String,Set<String>> fieldValueMap;
        HashSet<String> valueSet;

        //Security level map'
        String value = "";
        HashMap<String,String> alertValueMap;
        String tempDateString = "";

        HashSet<String> securityFoundInDbSet = new HashSet<>();

        for(String security: soMap.keySet()){
            so = soMap.get(security);
            if(securityFilterMap.containsKey(security)){
                fieldValueMap = securityFilterMap.get(security);
            }
            else{
                continue;
            }
            ArrayList<String> dateList = getDatesPrev(so,currentDateString,HEADER_DATE,daysToFilterFromStartDate);

            for(int i = 0; i< dateList.size(); i++){
                tempDateString = dateList.get(i);
                //
                // AlertObject ao = getAlertObjectFromRepo(tempDateString);
            }
//
//            //Update DB if SecurityObject record was found earlier
//            if(securityFoundInDbSet.contains(security)){
//                NITRITE_SECURITY_REPO.update(ao);
//            }
//            else{
//                NITRITE_SECURITY_REPO.insert(ao);
//            }
        }

    }

    public static HashMap<String,SecurityObject> upsertHistoricalData(HashMap<String,String> securityToLastPriceMap, HashMap<String, HashMap<String,String>> dataFromApi, String mainReportField){
        HashMap<String,SecurityObject> soMap = new HashMap<String,SecurityObject>();
        SecurityObject so;
        HashMap<String,String> fieldValueMap;
        HashMap<String,String> valueMap;
        String value = "";
        Integer dayRange = 0;
        LocalDate currentDate;
        String todayDateString = LocalDate.now().toString();
        //security key

        HashMap<String,HashMap<String,String>> securityAlertMap = new  HashMap<String,HashMap<String,String>>();
        HashMap<String,String> dateMap;
        HashMap<String,String> deltaDateMap;

        Set<String> sortedSecuritySet = new HashSet<String>();

        HashSet<String> securityFoundInDbSet = new HashSet<>();

        for(String security : dataFromApi.keySet()){

            //Get the SecurityObject from Database
            try {
                so = getSecurityObjectFromRepo(security);
            }catch(Exception e){
                e.printStackTrace();
                so = null;
            }

            //Keep track of which SecurityObjects are from DB
            //This SO will be merged with data from Bloomberg API
            if(so != null){
                securityFoundInDbSet.add(security);
            }
            else{
                //securityFoundInDbMap.put(security,false);
                so = new SecurityObject(security);
            }

            soMap.put(security, so);

            //Keep track of securities to be alerted
            if(securityAlertMap.containsKey(security)){
                dateMap = securityAlertMap.get(security);
            }
            else{
                dateMap = new HashMap<String,String>();
                securityAlertMap.put(security,dateMap);
            }

            //2D map, date, then field to value
            valueMap =  dataFromApi.get(security);

            HashSet<String> deltaDateSet = new HashSet<String>();

            //date key
            for(String dateKey: valueMap.keySet()){

                if(so.data.containsKey(dateKey)){fieldValueMap = so.data.get(dateKey);}
                else{
                    fieldValueMap = new HashMap<>();
                    so.data.put(dateKey, fieldValueMap);
                }
                //field value
                value = valueMap.get(dateKey);

                //ex. 20 day average field
                fieldValueMap.put(mainReportField,value);

                //Keep track of only new data pulled from API
                deltaDateSet.add(dateKey);

                HashMap<String,String> newFieldValueMap = new HashMap<>();

                //Exclude certain dates from calculation if they met the filter criteria earlier
                if(RUN_TYPE.equals(RUN_TYPE_TODAY) && dateKey.equals(todayDateString)){
                    //Add to deltaDateSet so today's record gets recognized for calculation
                    //Statistics should use PX_LAST if today
                    //deltaDateSet.clear();
                    deltaDateSet.add(todayDateString);
                }

                //Exclude certain dates from calculation if they met the filter criteria earlier
                if (EXCLUDE_SECURITIES_MAP.containsKey(so.securityId)) {
                    fieldValueMap.put(HEADER_EXCLUDE, "True");
                    fieldValueMap.put(HEADER_COMMENT, EXCLUDE_SECURITIES_MAP.get(so.securityId));
                } else {
                    fieldValueMap.put(HEADER_EXCLUDE, "False");
                }

            }

            //**** BEGIN MAIN LOGIC TO CALCULATE 20 DAY AVERAGE ******//

            ArrayList<String> dateList = new ArrayList(deltaDateSet);
            Collections.sort(dateList);

            for(int i = 0; i< dateList.size(); i++){
                String dateKey = dateList.get(i);
                currentDate = LocalDate.parse(dateKey);

                if(so.data.containsKey(dateKey)){
                    fieldValueMap = so.data.get(dateKey);
                }
                else{
                    fieldValueMap = new HashMap<>();
                    so.data.put(dateKey, fieldValueMap);
                }

                Statistics stats = getStatistics(so, currentDate.toString(), DATE_RANGE);

                if(stats == null){
                    fieldValueMap.put(HEADER_20_DAY_AVERAGE, "NULL");
                    fieldValueMap.put(HEADER_20_DAY_STDEV, "NULL");
                    fieldValueMap.put(HEADER_ALERT_01, "False");
                    fieldValueMap.put(HEADER_FILTER_01, "False");
                    fieldValueMap.put(HEADER_ALERT_01_DAYS, "0");
                }
                else{
                    Double twentyDayAverage = stats.getMean();
                    Double twentyDayStdDev = stats.getStdDev();

                    fieldValueMap.put(HEADER_20_DAY_AVERAGE, twentyDayAverage.toString());
                    fieldValueMap.put(HEADER_20_DAY_STDEV, twentyDayStdDev.toString());

                    filterAndAlert(so,currentDate.toString());

                    //Add back the new header values for each date after creating the alerts and filters
//                    if(!deltaDateMap.isEmpty()){
//                        dateMap.putAll(deltaDateMap);
//                        securityAlertSet.add(security);
//                    }
                }
            }

            //**** END LOGIC TO CALCULATE 20 DAY AVERAGE ******//

            //Trim entries to be within X days
            if(so.data.size() > CONFIG_DAYS_IN_DB){
                trimDateEntries(so,CONFIG_DAYS_IN_DB);
            }

            //Update DB if SecurityObject record was found earlier
            if(securityFoundInDbSet.contains(security)){
                NITRITE_SECURITY_REPO.update(so);
            }
            else{
                NITRITE_SECURITY_REPO.insert(so);
            }
        }
        return soMap;
    }

    public static SecurityObject getSecurityObjectFromRepo(String security){
        SecurityObject ssFromDb = null;
        org.dizitart.no2.objects.Cursor<SecurityObject> cursor = NITRITE_SECURITY_REPO.find( ObjectFilters.lte("securityId", security), FindOptions.sort("securityId", SortOrder.Ascending));
        for (SecurityObject soObject : cursor) {
           if(soObject.securityId.equals(security)){
               ssFromDb = soObject;
               break;
           }
        }
        return ssFromDb;
    }

    public static AlertObject getAlertObjectFromRepo(String date){
        AlertObject aoFromDb = null;
        org.dizitart.no2.objects.Cursor<AlertObject> cursor = NITRITE_ALERT_REPO.find( ObjectFilters.lte("dateId", date), FindOptions.sort("dateId", SortOrder.Ascending));
        for (AlertObject aoObject : cursor) {
            if(aoObject.dateId.equals(date)){
                aoFromDb = aoObject;
                break;
            }
        }
        return aoFromDb;
    }

    public static SecurityObject readFromExcel(String security){

        String directory = "./data/";
        String pathString = "";
        String dateString = "";
        BufferedWriter writer = null;
        CSVPrinter csvPrinter = null;
        pathString = directory + security + ".csv";
        SecurityObject ss = new SecurityObject(security);

        String[] headers = new String[7];
        headers[0] = "DATE";
        headers[1] = "30DAY_IMPVOL_100.0%MNY_DF";
        headers[2] = "20 DAY AVG";
        headers[3] = "20 DAY ST DEV";
        headers[4] = "FILTER 01";
        headers[5] = "ALERT 01";
        headers[6] = "ALERT 01 DAYS";
        Reader in = null;
        try {
            in = new FileReader(pathString);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Iterable<CSVRecord> records = null;
        try {
            records = CSVFormat.RFC4180.withHeader(headers).parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }


        HashMap<String,String> fieldMap = new HashMap<>();
        for (CSVRecord record : records) {
            String h00 = record.get(headers[0]);
            String h01 = record.get(headers[1]);
            String h02 = record.get(headers[2]);
            String h03 = record.get(headers[3]);
            String h04 = record.get(headers[4]);
            String h05 = record.get(headers[5]);
            String h06 = record.get(headers[6]);

            if(ss.data.containsKey(h00)){
                fieldMap = ss.data.get(h00);
            }
            else{
                fieldMap = new HashMap<>();
                ss.data.put(h00,fieldMap);
            }
            fieldMap.put(headers[0], h00);
            fieldMap.put(headers[1], h01);
            fieldMap.put(headers[2], h02);
            fieldMap.put(headers[3], h03);
            fieldMap.put(headers[4], h04);
            fieldMap.put(headers[5], h05);
            fieldMap.put(headers[6], h06);
        }

        return ss;
    }

    public static ArrayList<String> getSortedDateListDesc(HashMap<String,HashMap<String,String>> dataMap){
        ArrayList<String> dateList = new ArrayList(dataMap.keySet());
        Collections.sort(dateList);

        return dateList;
    }

    public static void writeSecurityObjectToCSV(SecurityObject so) throws IOException {
        String cleanedSecurity = String.valueOf(so.securityId).replace("/", " ") ;
        String pathString = PATH_DATA + cleanedSecurity + ".csv";
        String dateString = "";
        BufferedWriter writer = null;
        CSVPrinter csvPrinter = null;
        File file =new File(pathString);
        writer = new BufferedWriter(new FileWriter(file,false));
        csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(HEADER_DATE, THIRTY_DAY_IMPVOL, HEADER_20_DAY_AVERAGE, HEADER_20_DAY_STDEV,
                PX_LAST, HEADER_FILTER_01, HEADER_ALERT_01, HEADER_ALERT_01_DAYS, HEADER_EXCLUDE, HEADER_COMMENT));

        Utils.write("#Writing report for security: " + so.securityId);

        HashMap<String,String> valueMap = new HashMap<>();

        ArrayList<String> dateList = getSortedDateListDesc(so.data);
        for (int i =0; i < dateList.size(); i++) {
            dateString = dateList.get(i);
            valueMap  = so.data.get(dateString);
            try {
                csvPrinter.printRecord(dateString, valueMap.get(THIRTY_DAY_IMPVOL),valueMap.get(HEADER_20_DAY_AVERAGE), valueMap.get(HEADER_20_DAY_STDEV),valueMap.get(PX_LAST),
                        valueMap.get(HEADER_FILTER_01),valueMap.get(HEADER_ALERT_01), valueMap.get(HEADER_ALERT_01_DAYS),valueMap.get(HEADER_EXCLUDE), valueMap.get(HEADER_COMMENT));
            } catch (Exception e){
                Utils.write("Error writing line to file");
                e.printStackTrace();
            }
        }
        csvPrinter.flush();
        csvPrinter.close();
    }

    public static SecurityObject trimDateEntries(SecurityObject so, Integer maxDaysInStorage){

        ArrayList<String> dateList = getSortedDateListDesc(so.data);

        Integer daysToTrim = so.data.size() - maxDaysInStorage;

        String tempDateString = "";
        for(int i = 0; i< daysToTrim; i++){
            tempDateString = dateList.get(i);

            if(so.data.containsKey(tempDateString)){
                so.data.remove(tempDateString);
            }
        }
        return so;
    }

    public static HashMap<String,String> filterAndAlert(SecurityObject so, String currentDateString){

        HashMap<String,String> securityDateAlertMap = new HashMap<String,String>();

        Double stdev01 = 0.00;
        Double stdev02 = 0.00;
        Double avg01 = 0.00;
        Double avg02 = 0.00;
        Double dataPoint01 = 0.00;
        Double dataPoint02 = 0.00;
        Boolean isExcludedFromCalc01 = false;
        Boolean isExcludedFromCalc02 = false;
        Boolean isFilteredToday = false;
        Boolean isFilteredPrev = false;
        Integer alertCountToday = 0;
        Integer alertCountPrev = 0;

        LocalDate currentDate;
        LocalDate prevDate;

        Boolean isAlertToday = false;

        ArrayList<String> dateList = getDatesPrev(so,currentDateString,THIRTY_DAY_IMPVOL,2);

        HashMap<String,String> todayFieldValueMap = new HashMap<String,String>();
        HashMap<String,String> prevFieldValueMap = new HashMap<String,String>();
        String todayDateString = LocalDate.now().toString();

        //Today's Data
        try {
            currentDate = LocalDate.parse(currentDateString);
            prevDate = LocalDate.parse(dateList.get(0));
            todayFieldValueMap = so.data.get(currentDate.toString());

            stdev01 = Double.valueOf(todayFieldValueMap.get(HEADER_20_DAY_STDEV));
            avg01 = Double.valueOf(todayFieldValueMap.get(HEADER_20_DAY_AVERAGE));

            //isFilteredToday = Boolean.valueOf(todayFdo.get(THIRTY_DAY_IMPVOL));
            //alertCountToday = Integer.valueOf(todayFdo.get(HEADER_ALERT_01_DAYS));

            if(RUN_TYPE.equals(RUN_TYPE_TODAY)){
                dataPoint01 = Double.valueOf(todayFieldValueMap.get(PX_LAST));
            }
            else{
                dataPoint01 = Double.valueOf(todayFieldValueMap.get(THIRTY_DAY_IMPVOL));
            }
            prevFieldValueMap = so.data.get(prevDate.toString());
            stdev02 = Double.valueOf(prevFieldValueMap.get(HEADER_20_DAY_STDEV));
            avg02 = Double.valueOf(prevFieldValueMap.get(HEADER_20_DAY_AVERAGE));
            isExcludedFromCalc02 = Boolean.valueOf(prevFieldValueMap.get(HEADER_EXCLUDE));
            dataPoint02 = Double.valueOf(prevFieldValueMap.get(THIRTY_DAY_IMPVOL));
            //isFilteredPrev = Boolean.valueOf(prevFieldValueMap.get(HEADER_FILTER_01));
            alertCountPrev = Integer.valueOf(prevFieldValueMap.get(HEADER_ALERT_01_DAYS));
        }catch(Exception e){
            Utils.write("Error calculating average amounts for alert");
            todayFieldValueMap.put(HEADER_ALERT_01, "False");
            todayFieldValueMap.put(HEADER_FILTER_01, "False");
            todayFieldValueMap.put(HEADER_ALERT_01_DAYS, "0");
            return securityDateAlertMap;
        }

        //Custom filter for HEADER_ALERT_01
        //D1 > AVG2 + 4x STDEV2
        Double filterThreshold = avg02 + (4 * stdev02);
        isFilteredToday = dataPoint01 > filterThreshold ? true: false;

        //Current day, D1 meets the filter criteria
        if(isFilteredToday){
            //If the previous day, D2 also meets the filter criteria
            //Keep both D1 and D2 in data set, do not filter
            if (dataPoint02 > filterThreshold) {
                isFilteredToday = false;
                isFilteredPrev = false;
                prevFieldValueMap.put(HEADER_FILTER_01,isFilteredPrev.toString());
            }
            todayFieldValueMap.put(HEADER_FILTER_01,isFilteredToday.toString());
        }
        else{
            todayFieldValueMap.put(HEADER_FILTER_01,isFilteredToday.toString());
        }

        Double alertThreshold = avg02 + (2.5 * stdev02);
        isAlertToday = dataPoint01 > alertThreshold ? true : false;

        //TODO: handle when alertCountPrev = null
        if(isAlertToday){
            alertCountToday = alertCountPrev + 1;
            todayFieldValueMap.put(HEADER_ALERT_01_DAYS,alertCountToday.toString());

            //TODO: Make variables configurable
            if(alertCountToday >= 2 && alertCountToday <= 3){
                securityDateAlertMap.put(currentDateString,"True");
            }
        }
        else{
            todayFieldValueMap.put(HEADER_ALERT_01_DAYS,"0");
        }
        todayFieldValueMap.put(HEADER_ALERT_01,isAlertToday.toString());

        return todayFieldValueMap;
    }

    public static ArrayList<String> getDatesPrev(SecurityObject so, String currentDateString, String field,  Integer daysRangeBefore){
        LocalDate currentDate = LocalDate.parse(currentDateString);

        ArrayList<String> dateList = getSortedDateListDesc(so.data);
        String dateString = "";

        String tempDateString = "";
        Integer rightIndex = 0;
        Integer leftIndex = 0;

        ArrayList<String> deltaDateList = new ArrayList<String>();

        for(int i = 0; i< dateList.size(); i++){
            tempDateString = dateList.get(i);
            if(currentDateString.equals(tempDateString)){
                rightIndex = i;
                leftIndex = rightIndex - daysRangeBefore;
                break;
            }
        }

        for(int i = leftIndex + 1; i <= rightIndex; i++){
            dateString = dateList.get(i);
            deltaDateList.add(dateString);
        }

        return deltaDateList;
    }

    public static Statistics getStatistics(SecurityObject so, String currentDateString, Integer days){

        Utils.write("getStatistics - Date: " + currentDateString);
        Double currentAmount = 0.00;
        double[] values = new double[20];

        HashMap<String,String> fieldValueMap = new HashMap<String,String>();

       Boolean isExcludedFromCalc;

        ArrayList<String> dateList = getSortedDateListDesc(so.data);

        Collections.sort(dateList,Collections.reverseOrder());

        String dateString = "";
        String todayDateString = LocalDate.now().toString();
        String tempDateString = "";
        Integer rightIndex = 0;
        Integer leftIndex = 0;

        for(int i = 0; i< dateList.size(); i++){
            tempDateString = dateList.get(i);
            if(currentDateString.equals(tempDateString)){
                leftIndex = i;
                rightIndex = leftIndex + 20;
                break;
            }
        }

        //Not enough data to construct the 20 day average
        if(leftIndex < 0 || rightIndex >= dateList.size()){
            return null;
        }

        //TODO: HANDLE LATER
        if (dateList.size() < days){
            return null;
        }

        //If today's run is not the same as the first entry in the date list
        //No prices for Saturday & Sundays
        if(dateList.get(0) != currentDateString){

        }

        Integer listCounter = 0;
        for(int i = leftIndex; i < rightIndex; i++){

            dateString = dateList.get(i);
            fieldValueMap = so.data.get(dateString);
            isExcludedFromCalc = Boolean.valueOf(fieldValueMap.get(HEADER_EXCLUDE));

            if(isExcludedFromCalc){
                rightIndex++;
            }else{
                if(RUN_TYPE.equals(RUN_TYPE_TODAY) && dateString.equals(todayDateString)){
                    currentAmount = Double.valueOf(fieldValueMap.get(PX_LAST));
                }
                else{
                    currentAmount = Double.valueOf(fieldValueMap.get(THIRTY_DAY_IMPVOL));
                }

                values[listCounter] = currentAmount;
                listCounter++;
            }
            Utils.write("Add date: " + dateString);
            Utils.write("#" + listCounter + " - value: " + currentAmount.toString());
        }

        Statistics stats = new Statistics(values);
        return  stats;
    }

    //Only useful for queries with a single security and single field
    public static HashMap<String,String> createFlatMap(BloombergAPI.DataSetObject dso){
        BloombergAPI.SecurityDataObject sdo;
        BloombergAPI.FieldDataObject fdo;
        HashMap<String,String> valueMap;
        String value = "";

        HashMap<String,String> securityMap = new HashMap<String,String>();
        //Security level map
        for(String securityKey : dso.keySet()){
            sdo = dso.get(securityKey);

            //Field level map
            for(String fieldKey : sdo.keySet()){
                fdo = sdo.get(fieldKey);

                //Value level map, takes into account of fields that return multiple values
                for(String fieldDataKey: fdo.keySet()){
                    valueMap = fdo.get(fieldDataKey);
                    for(String fieldValueKey : valueMap.keySet()){
                        value = valueMap.get(fieldValueKey);
                        securityMap.put(securityKey,value);
                    }

                }
            }
        }
        return securityMap;
    }

    //Only useful for queries with a single security and single field
    public static HashMap<String, HashMap<String,String>> create2DMap(BloombergAPI.DataSetObject dso){
        BloombergAPI.SecurityDataObject sdo;
        BloombergAPI.FieldDataObject fdo;
        HashMap<String,String> dataMap;
        String value = "";

        HashMap<String, HashMap<String,String>> securityToFieldMap = new HashMap<String, HashMap<String,String>>();
        HashMap<String,String> fieldValueMap = new HashMap<String,String>();
        //Security level map
        for(String securityKey : dso.keySet()){
            sdo = dso.get(securityKey);

            if(securityToFieldMap.containsKey(securityKey)){
                fieldValueMap = securityToFieldMap.get(securityKey);
            }
            else{
                fieldValueMap = new HashMap<String,String>();
            }

            //Field level map
            for(String fieldKey : sdo.keySet()){
                fdo = sdo.get(fieldKey);

                //Value level map, takes into account of fields that return multiple values
                for(String fieldDataKey: fdo.keySet()){

                    dataMap = fdo.get(fieldDataKey);
                    for(String fieldValueKey : dataMap.keySet()){
                        value = dataMap.get(fieldValueKey);

                        if(RUN_TYPE.equals(RUN_TYPE_TODAY)){
                            fieldValueMap.put(LocalDate.now().toString(),value);
                        }else{
                            fieldValueMap.put(fieldDataKey,value);
                        }
                    }
                }

                if(RUN_TYPE.equals(RUN_TYPE_TODAY)){
                    securityKey = securityKey.replace(US_1M_100_VOL_LIVE_EQUITY,US_EQUITY).trim();
                }

                if(!securityToFieldMap.containsKey(securityKey)){
                    securityToFieldMap.put(securityKey,fieldValueMap);
                }
            }
        }
        return securityToFieldMap;
    }

    public static ArrayList<Set<String>> filterOnPrice(HashMap<String,String> dataMap, Double minPrice, Double maxPrice){
        ArrayList<Set<String>> filterList = new ArrayList<>();
        Set<String> includeSet = new HashSet<String>();
        Set<String> excludeSet = new HashSet<String>();
        Double value = 0.00;
        String valueString = "";
        if(minPrice != null){
            for(String key: dataMap.keySet()){
                valueString = dataMap.get(key);
                value =  Double.valueOf(dataMap.get(key));
                if( value > minPrice){
                    includeSet.add(key);
                }
                else{
                    excludeSet.add(key);
                }
            }
        }
        else if(maxPrice != null){
            for(String key: dataMap.keySet()){
                valueString = dataMap.get(key);
                value =  Double.valueOf(dataMap.get(key));
                if( value < maxPrice){
                    includeSet.add(key);
                }
                else{
                    excludeSet.add(key);
                }
            }
        }
        filterList.add(includeSet);
        filterList.add(excludeSet);
        return filterList;
    }

    public static class SecurityObject implements Serializable{
        @Id
        String securityId = "";
        HashMap<String,HashMap<String,String>> data ;

        public SecurityObject(){
            data = new HashMap<>();
        }

        public SecurityObject(String securityName){
            data = new HashMap<>();
            securityId = securityName;
        }
    }

    public static class AlertObject implements Serializable{
        @Id
        String dateId = "";
        HashMap<String,HashMap<String,String>> data ;

        public AlertObject(String date){
            data = new HashMap<>();
            dateId = date;
        }
    }

    public static class Statistics {
        double[] data;
        int size;

        public Statistics(double[] data) {
            this.data = data;
            size = data.length;
        }

        double getMean() {
            double sum = 0.0;
            for(double a : data)
                sum += a;
            return sum/size;
        }

        double getVariance() {
            double mean = getMean();
            double temp = 0;
            for(double a :data)
                temp += (a-mean)*(a-mean);
            return temp/(size-1);
        }

        double getStdDev() {
            return Math.sqrt(getVariance());
        }

        public double median() {
            Arrays.sort(data);

            if (data.length % 2 == 0) {
                return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
            }
            return data[data.length / 2];
        }
    }
//    public static class ReportSetObject {
//
//        public Set<String> securitySet;
//        public Set<String> fieldSet;
//        HashMap<String, SecurityDataObject> data;
//
//        public ReportSetObject(){
//            data = new HashMap<String,SecurityDataObject>();
//        }
//
//        public ReportSetObject(HashMap<String, SecurityDataObject> dataMap, Set<String> securityKeySet, Set<String> fieldKeySet){
//            securitySet = securityKeySet;
//            fieldSet = fieldKeySet;
//        }
//
//        public SecurityDataObject get(String key){
//            SecurityDataObject sdo;
//            if(data.containsKey(key)){
//                sdo = data.get(key);
//            }
//            else{
//                sdo = new SecurityDataObject();
//            }
//            return sdo;
//        }
//
//        public void put(String key,SecurityDataObject value){
//            data.put(key,value);
//        }
//        public void remove(String key){data.remove(key);}
//        public Boolean containsKey(String key){return data.containsKey(key);}
//        public Collection<SecurityDataObject> values(){return data.values();}
//        public Set<String> keySet(){return data.keySet();};
//    }
//    public static class SecurityDataObject{
//        public HashMap<String,FieldDataObject> data;
//
//        public SecurityDataObject(){
//            data = new HashMap<String,FieldDataObject>();
//        }
//        public SecurityDataObject(HashMap<String,FieldDataObject> securityDataMap){
//            data = securityDataMap;
//        }
//
//        //Returns HashMap<key,value> where key = Date, val = field value for requested Date
//        public FieldDataObject get(String key){
//            FieldDataObject fdo;
//            if(data.containsKey(key)){
//                fdo = data.get(key);
//            }
//            else{
//                fdo = new FieldDataObject();
//            }
//            return fdo;
//        }
//
//        public void put(String key, FieldDataObject value){
//            data.put(key,value);
//        }
//        public Boolean containsKey(String key){return data.containsKey(key);}
//        public Collection<FieldDataObject> values(){return data.values();}
//        public Set<String> keySet(){return data.keySet();}
//
//        public ArrayList<String> getSortedDateListDesc(){
//            ArrayList<String> dateList = new ArrayList(data.keySet());
//            Collections.sort(dateList);
//
//            return dateList;
//        }
//    }
//    public static class FieldDataObject{
//        public HashMap<String,String> data;
//
//        public FieldDataObject(){
//            data = new HashMap<String,String>();
//        }
//        public FieldDataObject(HashMap<String,String> fieldDataMap){
//            data = fieldDataMap;
//        }
//
//        //Returns HashMap<key,value> where key = Date, val = field value for requested Date
//        public String get(String key){
//            return data.get(key);
//        }
//
//        public void put(String key,String value){
//            data.put(key,value);
//        }
//        public Boolean containsKey(String key){return data.containsKey(key);}
//        public Collection<String> values(){return data.values();}
//        public Set<String> keySet(){return data.keySet();}
//    }
//public static void writeAllReports(ReportSetObject rso) throws IOException {
//    SecurityDataObject sdo = new SecurityDataObject();
//    FieldDataObject fdo = new FieldDataObject();
//    String directory = "./data/";
//    String pathString = "";
//    String dateString = "";
//    BufferedWriter writer = null;
//    CSVPrinter csvPrinter = null;
//    for(String security : rso.keySet()) {
//        pathString = directory +  security + ".csv";
//
//        File file =new File(pathString);
//        writer = new BufferedWriter(new FileWriter(file,false));
//        csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("DATE", THIRTY_DAY_IMPVOL, HEADER_20_DAY_AVERAGE, HEADER_20_DAY_STDEV, HEADER_FILTER_01, HEADER_ALERT_01, HEADER_ALERT_01_DAYS));
//
//        sdo = rso.get(security);
//
//        Utils.write("#Writing report for security: " + security);
//
//        ArrayList<String> dateList = sdo.getSortedDateListDesc();
//        for (int i =0; i < dateList.size(); i++) {
//            dateString = dateList.get(i);
//            fdo = sdo.get(dateString);
//            try {
//
//                csvPrinter.printRecord(dateString, fdo.get(THIRTY_DAY_IMPVOL),fdo.get(HEADER_20_DAY_AVERAGE), fdo.get(HEADER_20_DAY_STDEV), fdo.get(HEADER_FILTER_01),
//                        fdo.get(HEADER_ALERT_01), fdo.get(HEADER_ALERT_01_DAYS));
//
//            } catch (Exception e){
//                Utils.write("Error writing line to file");
//                e.printStackTrace();
//            }
//
//        }
//        csvPrinter.flush();
//    }
//}
}
