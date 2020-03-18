package com.bapi;
import com.utils.*;

import java.time.LocalDate;
import java.util.*;

public class BloombergAPI {

    BloombergSession session;

    public static final String FIELD_TYPE_DATE = "FIELD_TYPE_DATE";
    public static final String FIELD_TYPE_DOUBLE = "FIELD_TYPE_DOUBLE";
    public static final String FIELD_TYPE_STRING = "FIELD_TYPE_STRING";

    public static final String US_EQUITY = "US EQUITY";
    public static final String ONE_MONTH_VOL = "US 1M 100 VOL LIVE EQUITY";
    public static final String ONE_MONTH_IMPVOL = "30DAY_IMPVOL_100.0%MNY_DF";

    public static final String TICKER_CODE = "Member Ticker and Exchange Code";
    public static final String EXPIRATION_CODE = "Expiration";

    //Double fields
    public static final String PX_LAST = "PX_LAST";
    public static final String VOLUME_AVG_20D_TOTAL = "VOLUME_AVG_20D_TOTAL";
    public static final String ONE_M_100_VOL = "1M 100 VOL";
    public static final String THIRTY_DAY_IMPVOL_100_MNY_DF = "30DAY_IMPVOL_100.0%MNY_DF";

    // public static final String CHAIN_TICKERS = "CHAIN_TICKERS";
    //Date fields
    public static final String CHAIN_EXPIRATION_DATES = "CHAIN_EXPIRATION_DATES";
    public static final String DVD_EX_DT = "DVD_EX_DT";
    public static final String EXPECTED_REPORT_DT = "EXPECTED_REPORT_DT";
    public static final String ANNOUNCEMENT_DT = "ANNOUNCEMENT_DT";

    //Date field overrides
    public static final String CHAIN_PUT_CALL_TYPE_OVRD_P = "CHAIN_PUT_CALL_TYPE_OVRD";
    public static final String CHAIN_PERIODICITY_OVRD_ALL = "CHAIN_PERIODICITY_OVRD";
    public static final String CHAIN_STRIKE_PX_OVRD_ALL = "CHAIN_STRIKE_PX_OVRD";
    public static final String CHAIN_EXP_DT_OVRD_1F_5F = "CHAIN_EXP_DT_OVRD";

    public static Set<String> dateFieldSet;
    public static Set<String> priceFieldSet;
    public static Set<String> expirationDateSet;

    //public static Set<String> securitySet;
    //public static Set<String> fieldSet;

    public BloombergAPI(){
        session = new DefaultBloombergSession();
        session.start();
        initialize();
    }

    public void end(){
        session.stop();
    }

    public static void initialize(){
        dateFieldSet = new HashSet<String>();
        dateFieldSet.add(CHAIN_EXPIRATION_DATES);
        dateFieldSet.add(DVD_EX_DT);
        dateFieldSet.add(EXPECTED_REPORT_DT);
        dateFieldSet.add(ANNOUNCEMENT_DT);

        priceFieldSet = new HashSet<String>();
        priceFieldSet.add(PX_LAST);
        priceFieldSet.add(VOLUME_AVG_20D_TOTAL);
        priceFieldSet.add(ONE_MONTH_IMPVOL);
        priceFieldSet.add(THIRTY_DAY_IMPVOL_100_MNY_DF);

        expirationDateSet = buildExpirationDateSet();
    }

    public static Set<String> buildExpirationDateSet(){
        Set<String> dateSet = new HashSet<String>();
        dateSet.add(DVD_EX_DT);
        dateSet.add(CHAIN_EXPIRATION_DATES);
        return dateSet;
    }

    public RequestBuilder<ReferenceData> addOverrides(RequestBuilder<ReferenceData> request){
        ReferenceRequestBuilder rb =  (ReferenceRequestBuilder) request;
        rb.addOverride(CHAIN_PUT_CALL_TYPE_OVRD_P,"P");
        rb.addOverride(CHAIN_PERIODICITY_OVRD_ALL,"ALL");
        rb.addOverride(CHAIN_STRIKE_PX_OVRD_ALL,"ALL");
        rb.addOverride(CHAIN_EXP_DT_OVRD_1F_5F,"1F-5F");
        return rb;
    }

    public DataSetObject getReferenceData(Set<String> tickerSet, Set<String> fieldSet){
        ReferenceData referenceData = new ReferenceData();
        try {
            RequestBuilder<ReferenceData> hrb = new ReferenceRequestBuilder(tickerSet, fieldSet).addOverride(CHAIN_PUT_CALL_TYPE_OVRD_P, "P");
            hrb = addOverrides(hrb);
            referenceData = session.submit(hrb).get();

        }catch(Exception e){
            System.out.println(e.getStackTrace());
            System.out.println(e.getMessage());
            System.out.println("Reference request attempted.");
            session.stop();
        }

        DataSetObject dso = convertReferenceDataToMap(referenceData,tickerSet, fieldSet);
        return dso;
    }

    public DataSetObject getHistoricalData(Set<String> tickerSet, Set<String> fieldSet, LocalDate leftDate, LocalDate rightDate){
        HistoricalData historicalData = new HistoricalData();
        DataSetObject hdo = new DataSetObject();

        RequestBuilder<HistoricalData> hrb = new HistoricalRequestBuilder(tickerSet, fieldSet,leftDate,rightDate);
        try{
            historicalData = session.submit(hrb).get();
        }
        catch(Exception e){
            System.out.println(e.getStackTrace());
            System.out.println(e.getMessage());
            System.out.println("Historical request attempted.");
            session.stop();
        }

        hdo = convertHistoricalDataToMap(historicalData,tickerSet, fieldSet);
        return hdo;
    }

    public Set<String> getIndexMembers(String parentTicker){
        ReferenceData referenceData = new ReferenceData();
        Set<String> securitySet = new HashSet<String>();
        Utils.write("## Get all index members from parent: " + parentTicker);
        try {
            RequestBuilder<ReferenceData> hrb = new ReferenceRequestBuilder(parentTicker, "Indx_members");
            referenceData = session.submit(hrb).get();

        }catch(Exception e){
            System.out.println(e.getStackTrace());
            System.out.println(e.getMessage());
            System.out.println("Reference request attempted.");
            session.stop();
            referenceData = null;
        }

        if(referenceData == null) return securitySet;

        List<TypedObject> dataList =  referenceData.forSecurity(parentTicker).forField("Indx_members").asList();

        for (int i = 0; i < dataList.size(); i++){
            Map<String,String> tickerMap = (Map<String, String>) dataList.get(i);
            String tickerCode =  tickerMap.get(TICKER_CODE);
            String ticker = tickerCode.split(" ")[0];
            securitySet.add(ticker);
        }
        return securitySet;
    }

    public Set<String> updateTickerName(Set<String> tickerSet, String dataPointName){
        HashSet<String> updatedTickerSet = new HashSet<String> ();
        String appendedTicker = "";
        for(String ticker : tickerSet){
            appendedTicker = (ticker + " " + dataPointName).trim();
            updatedTickerSet.add(appendedTicker);
        }
        return updatedTickerSet;
    }

    public DataSetObject convertReferenceDataToMap(ReferenceData data, Set<String> securitySet, Set<String> fieldSet) {
        Map<String,String> tickerMap = new HashMap<String,String>();
        Boolean isList,isMap;
        isList = false;
        List<TypedObject> dataList;
        String key = "", value = "";

        DataSetObject dso = new DataSetObject();
        SecurityDataObject sdo =  new SecurityDataObject();
        FieldDataObject fdo =  new FieldDataObject();
        HashMap<String,String> fieldDataMap = new HashMap<String,String>();
        Map<String,String> dataMap = new HashMap<String,String>();
        LocalDate date = LocalDate.now();
        Boolean hasData = false;
        for(String security : securitySet){

            if(dso.containsKey(security)){sdo = dso.get(security);}
            else{
                sdo = new SecurityDataObject();
                dso.put(security, sdo);
            }

            for(String field : fieldSet){
                //Utils.write("Security: " + security + " - " + field);
                if(sdo.containsKey(field)){ fdo = sdo.get(field);}
                else{
                    fdo = new FieldDataObject();
                }

                try{
                    isList = data.forSecurity(security).forField(field).isList();
                }catch(Exception e){isList = false;}
                try{
                    isMap = data.forSecurity(security).forField(field).isList();
                }catch(Exception e){isMap = false;}

                try{
                    if(data.forSecurity(security).forField(field).get() != null) { hasData = true;
                    }else{continue;}
                }catch(Exception e){hasData = false; continue;}

                fieldDataMap = new HashMap<String,String>();
                if(isList){
                    dataList =  data.forSecurity(security).forField(field).asList();
                    for (int i = 0; i < dataList.size(); i++){
                        if(expirationDateSet.contains(field)){
                            dataMap = (Map<String, String>) dataList.get(i);
                            for(String dataMapKey : dataMap.keySet()){

                                if(fdo.containsKey(dataMapKey)){
                                    fieldDataMap = fdo.get(dataMapKey);
                                }
                                value = String.valueOf(dataMap.get(dataMapKey));
                                if (value.isEmpty() || value == null){continue;};
                                fieldDataMap.put(value,value);

                                if(!fdo.containsKey(dataMapKey)) {
                                    fdo.put(dataMapKey,fieldDataMap);
                                }
                                if(!sdo.containsKey(field)){
                                    fdo.put(dataMapKey,fieldDataMap);
                                    sdo.put(field,fdo);
                                }

                                //Utils.write("- Field: " + field + " - Value: " + value);
                            }
                        }
                    }
                }
                //To be handled, have not yet encountered a map type 7/21/2018
                else if(isMap){}
                else{

                    if(dateFieldSet.contains(field)){
                        value = data.forSecurity(security).forField(field).as(LocalDate.class).toString();
                    }
                    else if (priceFieldSet.contains(field)){
                        value = String.valueOf(data.forSecurity(security).forField(field).asDouble());
                    }
                    else{
                        value = data.forSecurity(security).forField(field).asString();
                    }
                    fieldDataMap.put(value,value);

                    if (value.isEmpty() || value == null){continue;};

                    if(!fdo.containsKey(value)) {
                        fdo.put(key,fieldDataMap);
                    }
                    if(!sdo.containsKey(field)){
                        fdo.put(key,fieldDataMap);
                        sdo.put(field,fdo);
                    }

                    //Utils.write("Field: " + field + " - Value: " + value);
                }
            }
        }
        return dso;
    }

    public DataSetObject convertHistoricalDataToMap(HistoricalData data, Set<String> securitySet, Set<String> fieldSet){

        DataSetObject dso = new DataSetObject();
        SecurityDataObject sdo =  new SecurityDataObject();
        FieldDataObject fdo =  new FieldDataObject();
        HashMap<String,String> dateMap;

        if(data == null || securitySet.isEmpty() || fieldSet.isEmpty()){
            return dso;
        }

        for(String security : securitySet){

            HistoricalData.ResultForSecurity securityData = data.forSecurity(security);
            if(dso.containsKey(security)){ sdo = dso.get(security);}
            else{
                sdo = new SecurityDataObject();
                dso.put(security, sdo);
            }

            //Utils.write("---Security: " + security + " ----");

            for(String field : fieldSet){

                if(sdo.containsKey(field)){fdo = sdo.get(field);}
                else{
                    fdo = new FieldDataObject();
                    sdo.put(field, fdo);
                }

                Map<LocalDate, TypedObject> securityFieldData = securityData.forField(field).get();

                for (Map.Entry<LocalDate, TypedObject> e : securityFieldData.entrySet()) {
                    LocalDate dt = e.getKey();
                    String dateKey = dt.toString();
                    double price = e.getValue().asDouble();
                    //Utils.write("Date: " + dt + " - Price: " + price);

                    if(fdo.containsKey(dateKey)){
                        dateMap = fdo.get(dateKey);
                        dateMap.put(dateKey,String.valueOf((price)));
                    }
                    else{
                        dateMap = new HashMap<String,String>();
                        dateMap.put(dateKey,String.valueOf((price)));
                        fdo.put(dateKey,dateMap);
                    }
                }
            }
        }
        return dso;
    }

    public class DataSetObject {

        public Set<String> securitySet;
        public Set<String> fieldSet;
        HashMap<String, SecurityDataObject> data;

        public DataSetObject(){
            data = new HashMap<String,SecurityDataObject>();
        }

        public DataSetObject(HashMap<String, SecurityDataObject> dataMap, Set<String> securityKeySet, Set<String> fieldKeySet){
            securitySet = securityKeySet;
            fieldSet = fieldKeySet;
        }

        public SecurityDataObject get(String key){
            SecurityDataObject sdo;
            if(data.containsKey(key)){
                sdo = data.get(key);
            }
            else{
                sdo = new SecurityDataObject();
            }
            return sdo;
        }

        public void put(String key,SecurityDataObject value){
            data.put(key,value);
        }
        public Boolean containsKey(String key){return data.containsKey(key);}
        public Collection<SecurityDataObject> values(){return data.values();}
        public Set<String> keySet(){return data.keySet();};
    }
    public class SecurityDataObject{
        public HashMap<String,FieldDataObject> data;

        public SecurityDataObject(){
            data = new HashMap<String,FieldDataObject>();
        }
        public SecurityDataObject(HashMap<String,FieldDataObject> securityDataMap){
            data = securityDataMap;
        }

        //Returns HashMap<key,value> where key = Date, val = field value for requested Date
        public FieldDataObject get(String key){
            FieldDataObject fdo;
            if(data.containsKey(key)){
                fdo = data.get(key);
            }
            else{
                fdo = new FieldDataObject();
            }
            return fdo;
        }

        public void put(String key, FieldDataObject value){
            data.put(key,value);
        }
        public Boolean containsKey(String key){return data.containsKey(key);}
        public Collection<FieldDataObject> values(){return data.values();}
        public Set<String> keySet(){return data.keySet();}
    }
    public class FieldDataObject{
        public HashMap<String,HashMap<String,String>> data;

        public FieldDataObject(){
            data = new HashMap<String,HashMap<String,String>>();
        }
        public FieldDataObject(HashMap<String,HashMap<String,String>> fieldDataMap){
            data = fieldDataMap;
        }

        //Returns HashMap<key,value> where key = Date, val = field value for requested Date
        public HashMap<String,String> get(String key){
            HashMap<String,String> dateMap;
            if(data.containsKey(key)){
                dateMap = data.get(key);
            }
            else{
                dateMap = new HashMap<String,String>();
            }
            return dateMap;
        }

        public void put(String key, HashMap<String, String> value){
            data.put(key,value);
        }
        public Boolean containsKey(String key){return data.containsKey(key);}
        public Collection<HashMap<String,String>> values(){return data.values();}
        public Set<String> keySet(){return data.keySet();}
    }
}
