/*
 * Copyright (C) 2012 - present by Yann Le Tallec.
 * Please see distribution for license.
 */
package com.assylias.jbloomberg;

import static com.assylias.jbloomberg.DateUtils.toOffsetDateTime;
import com.bloomberglp.blpapi.Datetime;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Name;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * A ResultParser to parse the responses received from the Bloomberg Session when sending a Historical Data request.
 *
 * This implementation is thread safe as the Bloomberg API might send results through more than one thread.
 */
final class IntradayBarResultParser extends AbstractResultParser<IntradayBarData> {

    private final static Logger logger = LoggerFactory.getLogger(IntradayBarResultParser.class);
    /**
     * The various element names
     */
    private static final Name BAR_DATA = new Name("barData");
    private static final Name BAR_TICK_DATA = new Name("barTickData");

    private final String security;

    /**
     * @param security the Bloomberg identifier of the security
     */
    public IntradayBarResultParser(String security) {
        this.security = security;
    }

    @Override
    protected void addFieldError(String field) {
        throw new UnsupportedOperationException("Intraday Bar Requests can't report a field exception");
    }

    @Override
    protected IntradayBarData getRequestResult() {
        return new IntradayBarData(security);
    }

    /**
     * Only the fields we are interested in - the numEvents and value fields will be discarded
     */
    private static enum BarTickDataElements {

        TIME("time"),
        OPEN("open"),
        HIGH("high"),
        LOW("low"),
        CLOSE("close"),
        VOLUME("volume"),
        NUM_EVENTS("numEvents");
        private final Name elementName;

        private BarTickDataElements(String elementName) {
            this.elementName = new Name(elementName);
        }

        private Name asName() {
            return elementName;
        }
    }

    @Override
    protected void parseResponseNoResponseError(Element response) {
        if (response.hasElement(BAR_DATA, true)) {
            Element barData = response.getElement(BAR_DATA);
            parseBarData(barData);
        }
    }

    private void parseBarData(Element barData) {
        if (barData.hasElement(BAR_TICK_DATA, true)) {
            Element barTickDataArray = barData.getElement(BAR_TICK_DATA);
            parseBarTickDataArray(barTickDataArray);
        }
    }

    /**
     * There should be no more error at this point and we can happily parse the interesting portion of the response
     *
     */
    private void parseBarTickDataArray(Element barTickDataArray) {
        int countData = barTickDataArray.numValues();
        for (int i = 0; i < countData; i++) {
            Element fieldData = barTickDataArray.getValueAsElement(i);
            Element field = fieldData.getElement(0);
            if (!BarTickDataElements.TIME.asName().equals(field.name())) {
                throw new AssertionError("Time field is supposed to be first but got: " + field.name());
            }
            Datetime dt = field.getValueAsDatetime();

            for (int j = 1; j < fieldData.numElements(); j++) {
                field = fieldData.getElement(j);
                addField(toOffsetDateTime(dt), field);
            }
        }
    }
}