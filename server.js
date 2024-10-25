const schedule = require("node-schedule");
const XLSX = require('xlsx');
const path = require('path');
const moment=require('moment')
const { POLL_ORACLE_DEV, NORMAL_ORACLE_DEV } = require('./oracle_connection');
const { pull_data } = require('./pull_data');
require('dotenv').config('.env')

async function main() {
try {
    const data = format_read_xl_data()
    const query = `
    INSERT INTO CUS.ADOR_MNR_SALESDATA (
    SL_NO_,
    INVOICE_NUMBER,
    INVOICE_DATE,
    INVOICE_STATUS,
    FINANCE_BOOK,
    CURRENCY,
    BILL_TO_CUST__CODE,
    BILL_TO_CUST__NAME,
    ITEM_CODE,
    ITEM_DESCRIPTION,
    INV__QTY,
    UOM,
    INV__RATE,
    LIST_PRICE,
    NET_RATE,
    STD__DISC_,
    STD__DISC_VALUE,
    ADD__DISC_,
    ADD__DISC_VALUE,
    OTHER_DISC__VALUE,
    STFI__,
    STFI_VALUE,
    STFI_FLAT_VALUE,
    OTHER_CHARGES_VALUE,
    TAXABLE_AMOUNT,
    CGST__,
    CGST_VALUE,
    SGST__,
    SGST_VALUE,
    IGST__,
    IGST_VALUE,
    GROSS_AMOUNT,
    NET_OF_STFI_AMOUNT,
    PACKSLIP_NO_,
    PACKSLIP_DATE,
    SHIP_TO_CUST__CODE,
    SHIP_TO_CUST__NAME,
    SALE_ORDER_NO_,
    SALE_ORDER_DATE,
    SALE_ORDER_STATUS,
    TOTAL_SALE_ORD__VALUE,
    END_USER_CUST__CODE,
    END_USER_CUST__NAME,
    SALE_ORDER_LINE_NO_,
    SALE_ORDER_QTY,
    QUOTATION_NO_,
    QUOTATION_DATE,
    TOTAL_QUOTATION_VALUE,
    QUOTATION_LINE_NO_,
    OPPORTUNITY_ID,
    LEAD_CATEGORY,
    LEAD_REV_POTENTIAL,
    SALES_PERSON_CODE,
    END_USER_ZONE,
    END_USER_AREA,
    END_USER_REGION,
    END_USER_DIVISION,
    END_USER_SBU,
    ITEM_GROUP_LEV__3,
    ITEM_GROUP_LEV__2,
    ITEM_GROUP_LEV__1,
    HSN_CODE___BILL_TO_GSTIN_NUMBER_
) VALUES (
    :SL_NO_,
    :INVOICE_NUMBER,
    TO_DATE(:INVOICE_DATE, 'YYYY-MM-DD'),
    :INVOICE_STATUS,
    :FINANCE_BOOK,
    :CURRENCY,
    :BILL_TO_CUST__CODE,
    :BILL_TO_CUST__NAME,
    :ITEM_CODE,
    :ITEM_DESCRIPTION,
    :INV__QTY,
    :UOM,
    :INV__RATE,
    :LIST_PRICE,
    :NET_RATE,
    :STD__DISC_,
    :STD__DISC_VALUE,
    :ADD__DISC_,
    :ADD__DISC_VALUE,
    :OTHER_DISC__VALUE,
    :STFI__,
    :STFI_VALUE,
    :STFI_FLAT_VALUE,
    :OTHER_CHARGES_VALUE,
    :TAXABLE_AMOUNT,
    :CGST__,
    :CGST_VALUE,
    :SGST__,
    :SGST_VALUE,
    :IGST__,
    :IGST_VALUE,
    :GROSS_AMOUNT,
    :NET_OF_STFI_AMOUNT,
    :PACKSLIP_NO_,
    :PACKSLIP_DATE,
    :SHIP_TO_CUST__CODE,
    :SHIP_TO_CUST__NAME,
    :SALE_ORDER_NO_,
    :SALE_ORDER_DATE,
    :SALE_ORDER_STATUS,
    :TOTAL_SALE_ORD__VALUE,
    :END_USER_CUST__CODE,
    :END_USER_CUST__NAME,
    :SALE_ORDER_LINE_NO_,
    :SALE_ORDER_QTY,
    :QUOTATION_NO_,
    :QUOTATION_DATE,
    :TOTAL_QUOTATION_VALUE,
    :QUOTATION_LINE_NO_,
    :OPPORTUNITY_ID,
    :LEAD_CATEGORY,
    :LEAD_REV_POTENTIAL,
    :SALES_PERSON_CODE,
    :END_USER_ZONE,
    :END_USER_AREA,
    :END_USER_REGION,
    :END_USER_DIVISION,
    :END_USER_SBU,
    :ITEM_GROUP_LEV__3,
    :ITEM_GROUP_LEV__2,
    :ITEM_GROUP_LEV__1,
    :HSN_CODE___BILL_TO_GSTIN_NUMBER_)`

    await POLL_ORACLE_DEV(query, data)
} catch (error) {
    console.log("error",error);
}
}

function read_xl_data() {
    const readXlsxFileAsJson = (filePath) => {
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[1];
        const sheet = workbook.Sheets[sheetName];
        const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: true });
        const columnNames = jsonData[1];
        const result = jsonData.slice(1).map(row => {
            const obj = {};
            columnNames.forEach((colName, index) => {
                obj[colName] = row[index]; 
            });
            return obj;
        });
        return result;
    };
    const filePath = './xlsx_data/MNR_YTD_SLS_2425.xlsx'; 
    const jsonData = readXlsxFileAsJson(filePath);
    return jsonData
}

function format_read_xl_data() {
    const data = read_xl_data()
    const formatted_data = data.slice(1).map((val) => {
        if (val?.["Sl.No."])
        {
           return {
               SL_NO_: val?.["Sl.No."],
               INVOICE_NUMBER: val?.["Invoice Number"],
               INVOICE_DATE: date_convert(val?.["Invoice Date"]),
               TAXABLE_AMOUNT: val?.["Taxable Amount"],
               NET_OF_STFI_AMOUNT: val?.["Net of STFI Amount"],
               INVOICE_STATUS: val?.["Invoice Status"],
               FINANCE_BOOK: val?.["Finance Book"],
               CURRENCY: val?.["Currency"],
               BILL_TO_CUST__CODE: val?.["Bill To Cust. Code"],
               BILL_TO_CUST__NAME: val?.["Bill To Cust. Name"],
               ITEM_CODE: val?.["Item Code"],
               ITEM_DESCRIPTION: val?.["Item Description"],
               INV__QTY: val?.["Inv. Qty"],
               UOM: val?.["UOM"],
               INV__RATE: val?.["Inv. Rate"],
               LIST_PRICE: val?.["List Price"],
               NET_RATE: val?.["Net Rate"],
               STD__DISC_: val?.["Std. Disc%"],
               STD__DISC_VALUE: val?.["Std. Disc Value"],
               ADD__DISC_: val?.["Add. Disc%"],
               ADD__DISC_VALUE: val?.["Add. Disc Value"],
               OTHER_DISC__VALUE: val?.["Other Disc. Value"],
               STFI__: val?.["STFI %"],
               STFI_VALUE: val?.["STFI Value"],
               STFI_FLAT_VALUE: val?.["STFI Flat Value"],
               OTHER_CHARGES_VALUE: val?.["Other Charges Value"],
               CGST__: val?.["CGST %"],
               CGST_VALUE: val?.["CGST Value"],
               SGST__: val?.["SGST %"],
               SGST_VALUE: val?.["SGST Value"],
               IGST__: val?.["IGST %"],
               IGST_VALUE: val?.["IGST Value"],
               GROSS_AMOUNT: val?.["Gross Amount"],
               PACKSLIP_NO_: val?.["Packslip No."],
               PACKSLIP_DATE: val?.["Packslip Date"],
               SHIP_TO_CUST__CODE: val?.["Ship To Cust. Code"],
               SHIP_TO_CUST__NAME: val?.["Ship to Cust. Name"],
               SALE_ORDER_NO_: val?.["Sale order No."],
               SALE_ORDER_DATE: val?.["Sale Order Date"],
               SALE_ORDER_STATUS: val?.["Sale Order Status"],
               TOTAL_SALE_ORD__VALUE: val?.["Total Sale Ord. Value"],
               END_USER_CUST__CODE: val?.["End User Cust. Code"],
               END_USER_CUST__NAME: val?.["End User Cust. Name"],
               SALE_ORDER_LINE_NO_: val?.["Sale Order Line No."],
               SALE_ORDER_QTY: val?.["Sale Order Qty"],
               QUOTATION_NO_: val?.["Quotation No."],
               QUOTATION_DATE: val?.["Quotation Date"],
               TOTAL_QUOTATION_VALUE: val?.["Total Quotation Value"],
               QUOTATION_LINE_NO_: val?.["Quotation Line No."],
               OPPORTUNITY_ID: val?.["Opportunity Id"],
               LEAD_CATEGORY: val?.["Lead Category"],
               LEAD_REV_POTENTIAL: val?.["Lead Rev Potential"],
               SALES_PERSON_CODE: val?.["Sales Person Code"],
               END_USER_ZONE: val?.["End User Zone"],
               END_USER_AREA: val?.["End user Area"],
               END_USER_REGION: val?.["End User Region"],
               END_USER_DIVISION: val?.["End User Division"],
               END_USER_SBU: val?.["End User SBU"],
               ITEM_GROUP_LEV__3: val?.["Item Group Lev. 3"],
               ITEM_GROUP_LEV__2: val?.["Item Group Lev. 2"],
               ITEM_GROUP_LEV__1: val?.["Item Group Lev. 1"],
               HSN_CODE___BILL_TO_GSTIN_NUMBER_: val?.["HSN Code / Bill To GSTIN Number "]
           }
       } else {
            console.log("sl is null and ignored", val?.["Sl.No."]);
           
       }
    })
    const filteredArray = formatted_data.filter(item => item !== undefined);

    return filteredArray
}

function date_convert(date) {   
    cellValue = XLSX.SSF.parse_date_code(date);
    const formattedDate = `${(cellValue.d)}-${cellValue.m}-${cellValue.y}`
    return moment(formattedDate,"D-M-YYYY").format("YYYY-MM-DD");
}

async function truncate_data() {
    try {
        await NORMAL_ORACLE_DEV("DELETE FROM cus.ADOR_MNR_SALESDATA ams WHERE SL_NO_ IS NOT null")  
    } catch (error) {
        console.log(error in truncate_data);
    }
}

schedule.scheduleJob("30 7 * * *", () => {
    console.log(`started at ${moment().format("DD-MM-YYYY hh:mm:ss")}`);
    truncate_data()
    pull_data()
    main()
    console.log(`ended at ${moment().format("DD-MM-YYYY hh:mm:ss")}`);
});